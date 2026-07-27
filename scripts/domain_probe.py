#!/usr/bin/env python3
"""Probe proxies against target domains via Mihomo /proxies/{name}/delay API."""

from __future__ import annotations

import argparse
import asyncio
import logging
import shutil
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import yaml

logger = logging.getLogger(__name__)


def load_targets(path: Path) -> list[str]:
    targets: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            targets.append(line)
    return targets


def write_temp_config(proxies: list[dict[str, Any]], path: Path, controller: str) -> None:
    cfg = {
        "mixed-port": 17890,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "warning",
        "external-controller": controller,
        "proxies": proxies,
        "rules": ["MATCH,DIRECT"],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh, allow_unicode=True, sort_keys=False)


async def wait_for_controller(base: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=2.0) as client:
        while time.monotonic() < deadline:
            try:
                r = await client.get(f"{base}/version")
                if r.status_code == 200:
                    return
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(0.25)
    raise RuntimeError(f"Mihomo controller not ready at {base}")


async def probe_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    base: str,
    proxy_name: str,
    domain: str,
    timeout_ms: int,
) -> tuple[str, str, bool]:
    url = f"https://{domain}/"
    api = (
        f"{base}/proxies/{quote(proxy_name, safe='')}/delay"
        f"?url={quote(url, safe='')}&timeout={timeout_ms}&expected=100-599"
    )
    async with sem:
        try:
            r = await client.get(api)
            if r.status_code != 200:
                return proxy_name, domain, False
            data = r.json()
            ok = isinstance(data, dict) and "delay" in data and data["delay"] is not None
            return proxy_name, domain, bool(ok)
        except Exception:  # noqa: BLE001
            return proxy_name, domain, False


async def probe_all(
    proxies: list[dict[str, Any]],
    targets: list[str],
    controller: str = "127.0.0.1:9090",
    concurrency: int = 40,
    timeout_ms: int = 8000,
    min_success_ratio: float = 0.7,
    mihomo_bin: str = "mihomo",
    work_dir: Path = Path("work"),
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not proxies:
        return [], {"probed": 0, "passed": 0, "targets": len(targets)}
    if not targets:
        logger.warning("No targets; accepting all %d proxies", len(proxies))
        return proxies, {"probed": len(proxies), "passed": len(proxies), "targets": 0}

    cfg_path = work_dir / "mihomo-probe.yaml"
    write_temp_config(proxies, cfg_path, controller)
    bin_path = shutil.which(mihomo_bin) or mihomo_bin
    proc = subprocess.Popen(
        [bin_path, "-f", str(cfg_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base = f"http://{controller}"
    try:
        await wait_for_controller(base)
        sem = asyncio.Semaphore(concurrency)
        # Long client timeout: each delay call can take up to timeout_ms.
        timeout = httpx.Timeout(timeout_ms / 1000.0 + 5.0)
        results: dict[str, list[bool]] = defaultdict(list)

        async with httpx.AsyncClient(timeout=timeout) as client:
            tasks = [
                probe_one(client, sem, base, str(p["name"]), domain, timeout_ms)
                for p in proxies
                for domain in targets
            ]
            logger.info(
                "Probing %d proxies x %d domains = %d calls (concurrency=%d)",
                len(proxies),
                len(targets),
                len(tasks),
                concurrency,
            )
            for coro in asyncio.as_completed(tasks):
                name, _domain, ok = await coro
                results[name].append(ok)

        passed: list[dict[str, Any]] = []
        for proxy in proxies:
            name = str(proxy["name"])
            outcomes = results.get(name, [])
            if not outcomes:
                continue
            ratio = sum(1 for x in outcomes if x) / len(outcomes)
            if ratio >= min_success_ratio:
                passed.append(proxy)

        stats = {
            "probed": len(proxies),
            "passed": len(passed),
            "targets": len(targets),
            "min_success_ratio": min_success_ratio,
        }
        logger.info(
            "Domain probe: %d/%d proxies passed (>= %.0f%% of %d targets)",
            len(passed),
            len(proxies),
            min_success_ratio * 100,
            len(targets),
        )
        return passed, stats
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="work/speed-filtered.yaml")
    parser.add_argument("--targets", default="data/group-a-targets.txt")
    parser.add_argument("--output", default="work/domain-filtered.yaml")
    parser.add_argument("--mihomo-bin", default="mihomo")
    parser.add_argument("--controller", default="127.0.0.1:9090")
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--timeout-ms", type=int, default=8000)
    parser.add_argument("--min-ratio", type=float, default=0.7)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    data = yaml.safe_load(Path(args.input).read_text(encoding="utf-8")) or {}
    proxies = data.get("proxies") if isinstance(data, dict) else None
    if not isinstance(proxies, list):
        proxies = []
    targets = load_targets(Path(args.targets))

    passed, _stats = asyncio.run(
        probe_all(
            proxies,
            targets,
            controller=args.controller,
            concurrency=args.concurrency,
            timeout_ms=args.timeout_ms,
            min_success_ratio=args.min_ratio,
            mihomo_bin=args.mihomo_bin,
        )
    )
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"proxies": passed}, fh, allow_unicode=True, sort_keys=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
