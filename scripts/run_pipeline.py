#!/usr/bin/env python3
"""Orchestrate fetch → sanitize → speedtest → domain probe → write backup-nodes.yaml."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import re
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
DEFAULT_SPEEDTEST_CAP = 500
PROXY_INDEX_RE = re.compile(r"proxy\s+(\d+)\s*:", re.IGNORECASE)


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


def _dump_proxies(path: Path, proxies: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"proxies": proxies}, fh, allow_unicode=True, sort_keys=False)


def run_speedtest(
    proxies: list[dict[str, Any]],
    input_yaml: Path,
    output_yaml: Path,
    max_latency: str = "2000ms",
    concurrent: int = 12,
    timeout: str = "6s",
    max_load_retries: int = 40,
) -> list[dict[str, Any]]:
    """Latency-only filter (-fast). Retries after dropping proxies that fail to load."""
    bin_name = shutil.which("clash-speedtest")
    if not bin_name:
        go_bin = Path.home() / "go" / "bin" / "clash-speedtest"
        if go_bin.exists():
            bin_name = str(go_bin)
        else:
            raise FileNotFoundError("clash-speedtest not found in PATH or ~/go/bin")

    candidates = list(proxies)
    last_err = ""

    for attempt in range(max_load_retries + 1):
        if not candidates:
            logger.info("No candidates left after load-failure retries")
            return []

        _dump_proxies(input_yaml, candidates)
        if output_yaml.exists():
            output_yaml.unlink()

        cmd = [
            bin_name,
            "-c",
            str(input_yaml),
            "-output",
            str(output_yaml),
            "-fast",
            "-max-latency",
            max_latency,
            "-concurrent",
            str(concurrent),
            "-timeout",
            timeout,
        ]
        logger.info(
            "Running speedtest attempt %d (%d proxies): %s",
            attempt + 1,
            len(candidates),
            " ".join(cmd),
        )
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        if stdout:
            logger.info(stdout[-4000:])
        if stderr:
            logger.warning(stderr[-4000:])

        combined = f"{stdout}\n{stderr}"
        load_failed = "load proxies failed" in combined.lower()

        if result.returncode == 0 and not load_failed:
            if not output_yaml.exists():
                logger.info("Speedtest finished cleanly with no output file (0 proxies passed)")
                return []
            data = yaml.safe_load(output_yaml.read_text(encoding="utf-8")) or {}
            kept = data.get("proxies") if isinstance(data, dict) else None
            if not isinstance(kept, list):
                return []
            logger.info("Speedtest kept %d proxies", len(kept))
            return kept

        # Try to drop the specific bad proxy and retry.
        m = PROXY_INDEX_RE.search(combined)
        if load_failed and m:
            # clash-speedtest indexes appear 1-based in messages like "proxy 522:"
            idx = int(m.group(1))
            drop_at = idx - 1 if idx >= 1 else idx
            if 0 <= drop_at < len(candidates):
                bad = candidates.pop(drop_at)
                logger.warning(
                    "Dropping unloadable proxy index=%d name=%r and retrying (%d left)",
                    idx,
                    bad.get("name"),
                    len(candidates),
                )
                last_err = combined.strip()[:500]
                continue

        last_err = (stderr or stdout or "no output")[:500]
        raise SpeedtestCrashError(
            f"speedtest crashed (exit={result.returncode}, load_failed={load_failed}): {last_err}"
        )

    raise SpeedtestCrashError(
        f"speedtest crashed after {max_load_retries} load retries: {last_err}"
    )


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
    capped_path = work / "speedtest-input.yaml"
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
    _dump_proxies(sanitized_path, sanitized)

    if not sanitized:
        write_output([], out_path, note="no proxies after sanitize")
        Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
        return 0

    candidates = list(sanitized)
    if args.speedtest_cap > 0 and len(candidates) > args.speedtest_cap:
        rng = random.Random(42)
        rng.shuffle(candidates)
        candidates = candidates[: args.speedtest_cap]
        logger.info("Capped speedtest candidates to %d", len(candidates))
    stats["speedtest_candidates"] = len(candidates)

    if args.skip_speedtest:
        speed_proxies = candidates
        stats["speedtest"] = {"passed": len(speed_proxies), "skipped": True, "mode": "skipped"}
    else:
        try:
            speed_proxies = run_speedtest(candidates, capped_path, speed_path)
        except SpeedtestCrashError:
            logger.exception("speedtest crashed")
            Path(args.stats_json).parent.mkdir(parents=True, exist_ok=True)
            Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
            return 2
        stats["speedtest"] = {
            "passed": len(speed_proxies),
            "skipped": False,
            "mode": "fast-latency",
        }
        _dump_proxies(speed_path, speed_proxies)

    if not speed_proxies:
        write_output([], out_path, note="no proxies passed speedtest latency filter")
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
