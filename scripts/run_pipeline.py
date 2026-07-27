#!/usr/bin/env python3
"""Orchestrate fetch → sanitize → speedtest → domain probe → write backup-nodes.yaml."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from domain_probe import load_targets, probe_all  # noqa: E402
from fetch_and_merge import fetch_and_merge  # noqa: E402
from sanitize_proxies import sanitize_proxies  # noqa: E402

logger = logging.getLogger(__name__)

# Cap candidates before speedtest so a 90-minute Actions job stays reliable.
DEFAULT_SPEEDTEST_CAP = 800


class SpeedtestCrashError(RuntimeError):
    """clash-speedtest exited non-zero or failed to load the config."""


def write_output(proxies: list[dict[str, Any]], path: Path, note: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    header = [
        f"# last-updated: {stamp}",
        f"# node-count: {len(proxies)}",
    ]
    if note:
        header.append(f"# note: {note}")
    body = yaml.safe_dump({"proxies": proxies}, allow_unicode=True, sort_keys=False)
    path.write_text("\n".join(header) + "\n" + body, encoding="utf-8")
    logger.info("Wrote %s (%d nodes)", path, len(proxies))


def run_speedtest(
    input_yaml: Path,
    output_yaml: Path,
    max_latency: str = "2000ms",
    # 2 MB/s is unrealistic for most free nodes from Actions runners; 0.2 ≈ 200 KB/s.
    min_download_speed: float = 0.2,
    concurrent: int = 8,
    timeout: str = "8s",
    download_size: int = 10,
) -> list[dict[str, Any]]:
    bin_name = shutil.which("clash-speedtest")
    if not bin_name:
        go_bin = Path.home() / "go" / "bin" / "clash-speedtest"
        if go_bin.exists():
            bin_name = str(go_bin)
        else:
            raise FileNotFoundError("clash-speedtest not found in PATH or ~/go/bin")

    if output_yaml.exists():
        output_yaml.unlink()

    output_yaml.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        bin_name,
        "-c",
        str(input_yaml),
        "-output",
        str(output_yaml),
        "-max-latency",
        max_latency,
        "-min-download-speed",
        str(min_download_speed),
        "-concurrent",
        str(concurrent),
        "-timeout",
        timeout,
        "-download-size",
        str(download_size),
    ]
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    stdout = (result.stdout or "")[-4000:]
    stderr = (result.stderr or "")[-4000:]
    if stdout:
        logger.info(stdout)
    if stderr:
        logger.warning(stderr)

    combined = f"{result.stdout or ''}\n{result.stderr or ''}"
    load_failed = "load proxies failed" in combined.lower()

    if result.returncode != 0 or load_failed:
        raise SpeedtestCrashError(
            f"speedtest crashed (exit={result.returncode}, load_failed={load_failed}): "
            f"{(stderr or stdout or 'no output')[:500]}"
        )

    if not output_yaml.exists():
        # Clean run but nothing written — treat as genuine empty filter result.
        logger.info("Speedtest finished cleanly with no output file (0 proxies passed filters)")
        return []

    data = yaml.safe_load(output_yaml.read_text(encoding="utf-8")) or {}
    proxies = data.get("proxies") if isinstance(data, dict) else None
    if not isinstance(proxies, list):
        return []
    logger.info("Speedtest kept %d proxies", len(proxies))
    return proxies


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", default=str(ROOT / "sources.txt"))
    parser.add_argument("--targets", default=str(ROOT / "data" / "group-a-targets.txt"))
    parser.add_argument("--work-dir", default=str(ROOT / "work"))
    parser.add_argument("--output", default=str(ROOT / "output" / "backup-nodes.yaml"))
    parser.add_argument("--mihomo-bin", default=os.environ.get("MIHOMO_BIN", "mihomo"))
    parser.add_argument("--skip-speedtest", action="store_true")
    parser.add_argument("--skip-domain-probe", action="store_true")
    parser.add_argument("--speedtest-cap", type=int, default=DEFAULT_SPEEDTEST_CAP)
    parser.add_argument("--stats-json", default=str(ROOT / "work" / "stats.json"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    merged_path = work / "merged.yaml"
    sanitized_path = work / "sanitized.yaml"
    speed_path = work / "speed-filtered.yaml"
    out_path = Path(args.output)

    stats: dict[str, Any] = {}

    merge_stats = fetch_and_merge(Path(args.sources), merged_path)
    stats["merge"] = merge_stats

    if merge_stats["proxies_merged"] == 0:
        write_output([], out_path, note="no proxies after merge")
        Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
        return 0

    data = yaml.safe_load(merged_path.read_text(encoding="utf-8")) or {}
    all_proxies = data.get("proxies") if isinstance(data, dict) else None
    if not isinstance(all_proxies, list):
        all_proxies = []

    sanitized, sanitize_stats = sanitize_proxies(all_proxies)
    stats["sanitize"] = sanitize_stats
    with sanitized_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"proxies": sanitized}, fh, allow_unicode=True, sort_keys=False)

    if not sanitized:
        write_output([], out_path, note="no proxies after sanitize")
        Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
        return 0

    # Shuffle + cap after sanitize (installed clash-speedtest may lack -early-stop).
    candidates = list(sanitized)
    if args.speedtest_cap > 0 and len(candidates) > args.speedtest_cap:
        rng = random.Random(42)
        rng.shuffle(candidates)
        candidates = candidates[: args.speedtest_cap]
        logger.info("Capped speedtest candidates to %d", len(candidates))
    stats["speedtest_candidates"] = len(candidates)

    capped_path = work / "speedtest-input.yaml"
    with capped_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"proxies": candidates}, fh, allow_unicode=True, sort_keys=False)

    if args.skip_speedtest:
        speed_proxies = candidates
        stats["speedtest"] = {"passed": len(speed_proxies), "skipped": True}
    else:
        try:
            speed_proxies = run_speedtest(capped_path, speed_path)
        except SpeedtestCrashError:
            logger.exception("speedtest crashed")
            Path(args.stats_json).parent.mkdir(parents=True, exist_ok=True)
            Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
            return 2
        stats["speedtest"] = {"passed": len(speed_proxies), "skipped": False}
        with speed_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump({"proxies": speed_proxies}, fh, allow_unicode=True, sort_keys=False)

    if not speed_proxies:
        # Genuine empty: sanitize + speedtest both ran cleanly, nothing met thresholds.
        write_output([], out_path, note="no proxies passed speedtest thresholds")
        Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
        logger.info("Pipeline stats: %s", json.dumps(stats))
        return 0

    targets = load_targets(Path(args.targets))
    if args.skip_domain_probe:
        passed = speed_proxies
        stats["domain_probe"] = {"passed": len(passed), "skipped": True, "targets": len(targets)}
    else:
        passed, probe_stats = asyncio.run(
            probe_all(
                speed_proxies,
                targets,
                mihomo_bin=args.mihomo_bin,
                work_dir=work,
            )
        )
        stats["domain_probe"] = {**probe_stats, "skipped": False}

    write_output(passed, out_path)
    Path(args.stats_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
    logger.info("Pipeline stats: %s", json.dumps(stats))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
