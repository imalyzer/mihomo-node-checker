#!/usr/bin/env python3
"""Orchestrate sticky pool: fetch → sanitize → light-recheck old → full-test new → dual outputs."""

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
from pool_state import (  # noqa: E402
    LIGHT_TARGETS,
    bootstrap_state_from_outputs,
    fp_key,
    partition_stable_fresh,
    save_state,
    split_newcomers,
    utc_now,
)
from sanitize_proxies import sanitize_proxies, strip_internal  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_SPEEDTEST_CAP = 500
PROXY_INDEX_RE = re.compile(r"proxy\s+(\d+)\s*:", re.IGNORECASE)
LIGHT_MIN_RATIO = 0.5
FULL_MIN_RATIO = 0.7


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

        m = PROXY_INDEX_RE.search(combined)
        if load_failed and m:
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
    parser.add_argument("--output-dir", default=str(ROOT / "output"))
    parser.add_argument("--state", default=str(ROOT / "data" / "pool-state.json"))
    parser.add_argument("--mihomo-bin", default=os.environ.get("MIHOMO_BIN", "mihomo"))
    parser.add_argument("--skip-speedtest", action="store_true")
    parser.add_argument("--skip-domain-probe", action="store_true")
    parser.add_argument("--speedtest-cap", type=int, default=DEFAULT_SPEEDTEST_CAP)
    parser.add_argument("--stats-json", default=str(ROOT / "work" / "stats.json"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    backup_path = out_dir / "backup-nodes.yaml"
    stable_path = out_dir / "stable-nodes.yaml"
    fresh_path = out_dir / "fresh-nodes.yaml"
    state_path = Path(args.state)

    merged_path = work / "merged.yaml"
    sanitized_path = work / "sanitized.yaml"
    speed_path = work / "speed-filtered.yaml"
    capped_path = work / "speedtest-input.yaml"

    stats: dict[str, Any] = {}

    # --- Sticky pool load ---
    state = bootstrap_state_from_outputs(state_path, backup_path, stable_path, fresh_path)
    pool_nodes: dict[str, dict[str, Any]] = dict(state.get("nodes") or {})
    previous_proxies = [dict(e["proxy"]) for e in pool_nodes.values() if isinstance(e.get("proxy"), dict)]
    pool_fps = set(pool_nodes.keys())
    logger.info("Loaded sticky pool: %d previous nodes", len(previous_proxies))
    stats["pool_before"] = len(previous_proxies)

    # --- Fetch / sanitize newcomers source material ---
    merge_stats = fetch_and_merge(Path(args.sources), merged_path)
    stats["merge"] = merge_stats

    data = yaml.safe_load(merged_path.read_text(encoding="utf-8")) if merged_path.exists() else {}
    all_proxies = data.get("proxies") if isinstance(data, dict) else None
    if not isinstance(all_proxies, list):
        all_proxies = []

    sanitized, sanitize_stats = sanitize_proxies(all_proxies)
    stats["sanitize"] = sanitize_stats
    _dump_proxies(sanitized_path, sanitized)

    _known_in_source, newcomers = split_newcomers(sanitized, pool_fps)
    logger.info(
        "Candidates: %d sanitized, %d already in pool, %d newcomers",
        len(sanitized),
        len(_known_in_source),
        len(newcomers),
    )

    # Cap newcomers for full test budget.
    if args.speedtest_cap > 0 and len(newcomers) > args.speedtest_cap:
        rng = random.Random(42)
        rng.shuffle(newcomers)
        newcomers = newcomers[: args.speedtest_cap]
        logger.info("Capped newcomers for full test to %d", len(newcomers))
    stats["newcomers"] = len(newcomers)

    # --- Light recheck previous pool ---
    survivors: list[dict[str, Any]] = []
    if previous_proxies:
        if args.skip_domain_probe:
            survivors = previous_proxies
            stats["light_recheck"] = {"probed": len(previous_proxies), "passed": len(survivors), "skipped": True}
        else:
            survivors, light_stats = asyncio.run(
                probe_all(
                    previous_proxies,
                    LIGHT_TARGETS,
                    controller="127.0.0.1:9090",
                    mihomo_bin=args.mihomo_bin,
                    work_dir=work,
                    min_success_ratio=LIGHT_MIN_RATIO,
                    concurrency=40,
                    timeout_ms=6000,
                )
            )
            stats["light_recheck"] = {**light_stats, "skipped": False}
            logger.info(
                "Light recheck: %d/%d previous nodes still healthy",
                len(survivors),
                len(previous_proxies),
            )
    else:
        stats["light_recheck"] = {"probed": 0, "passed": 0, "skipped": False}

    # Update streaks for survivors; drop dead from pool.
    survivor_fps = {fp_key(p) for p in survivors}
    new_pool: dict[str, dict[str, Any]] = {}
    for key, entry in pool_nodes.items():
        if key not in survivor_fps:
            continue
        proxy = strip_internal(dict(entry.get("proxy") or {}))
        # Refresh proxy object from survivor list (may have renamed).
        for s in survivors:
            if fp_key(s) == key:
                proxy = strip_internal(s)
                break
        streak = int(entry.get("streak") or 0) + 1
        new_pool[key] = {
            "streak": streak,
            "last_ok_at": utc_now(),
            "proxy": proxy,
        }

    # --- Full test newcomers only ---
    new_passers: list[dict[str, Any]] = []
    if newcomers:
        if args.skip_speedtest:
            speed_proxies = newcomers
            stats["speedtest"] = {"passed": len(speed_proxies), "skipped": True, "mode": "skipped"}
        else:
            try:
                speed_proxies = run_speedtest(newcomers, capped_path, speed_path)
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

        targets = load_targets(Path(args.targets))
        if args.skip_domain_probe:
            new_passers = speed_proxies
            stats["domain_probe"] = {
                "passed": len(new_passers),
                "skipped": True,
                "targets": len(targets),
            }
        elif speed_proxies:
            new_passers, probe_stats = asyncio.run(
                probe_all(
                    speed_proxies,
                    targets,
                    controller="127.0.0.1:9091",
                    mihomo_bin=args.mihomo_bin,
                    work_dir=work,
                    min_success_ratio=FULL_MIN_RATIO,
                )
            )
            stats["domain_probe"] = {**probe_stats, "skipped": False}
        else:
            stats["domain_probe"] = {"passed": 0, "skipped": False, "targets": 0, "probed": 0}
    else:
        stats["speedtest"] = {"passed": 0, "skipped": False, "mode": "no-newcomers"}
        stats["domain_probe"] = {"passed": 0, "skipped": False, "targets": 0, "probed": 0}

    for proxy in new_passers:
        clean = strip_internal(proxy)
        key = fp_key(clean)
        new_pool[key] = {
            "streak": 1,
            "last_ok_at": utc_now(),
            "proxy": clean,
        }

    stats["pool_after"] = len(new_pool)
    stats["new_passers"] = len(new_passers)

    save_state(state_path, new_pool)
    stable, fresh, union = partition_stable_fresh(new_pool)

    write_output(stable, stable_path, note="streak>=3 sticky survivors")
    write_output(fresh, fresh_path, note="streak<3 or newly accepted")
    write_output(union, backup_path, note="union of stable+fresh")

    Path(args.stats_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.stats_json).write_text(json.dumps(stats, indent=2), encoding="utf-8")
    logger.info("Pipeline stats: %s", json.dumps(stats))
    logger.info(
        "Outputs: stable=%d fresh=%d backup=%d",
        len(stable),
        len(fresh),
        len(union),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
