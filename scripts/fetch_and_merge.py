#!/usr/bin/env python3
"""Download free Clash/Mihomo sources, merge proxies, and strong-dedupe."""

from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx
import yaml

logger = logging.getLogger(__name__)

UA = "mihomo-node-checker/1.0"


def load_sources(path: Path) -> list[str]:
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def _nested_get(obj: dict[str, Any], *keys: str) -> Any:
    cur: Any = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def proxy_fingerprint(proxy: dict[str, Any]) -> tuple:
    """Strong identity so Cloudflare anycast + different uuid/path stay distinct."""
    ptype = str(proxy.get("type", "")).lower()
    server = str(proxy.get("server", "")).lower()
    port = proxy.get("port", "")
    identity = (
        proxy.get("uuid")
        or proxy.get("password")
        or proxy.get("private-key")
        or proxy.get("token")
        or ""
    )
    path = (
        _nested_get(proxy, "ws-opts", "path")
        or _nested_get(proxy, "h2-opts", "path")
        or _nested_get(proxy, "http-opts", "path")
        or _nested_get(proxy, "grpc-opts", "grpc-service-name")
        or ""
    )
    if isinstance(path, list):
        path = path[0] if path else ""
    network = str(proxy.get("network") or "")
    servername = str(proxy.get("servername") or proxy.get("sni") or "")
    reality_pk = _nested_get(proxy, "reality-opts", "public-key") or ""
    reality_sid = _nested_get(proxy, "reality-opts", "short-id") or ""
    return (
        ptype,
        server,
        str(port),
        str(identity),
        str(path),
        network,
        servername,
        str(reality_pk),
        str(reality_sid),
    )


def _source_label(url: str) -> str:
    path = unquote(urlparse(url).path)
    return path.rsplit("/", 2)[-1] if path else url


def fetch_one(client: httpx.Client, url: str) -> tuple[str, list[dict[str, Any]] | None, str | None]:
    label = _source_label(url)
    try:
        resp = client.get(url)
        resp.raise_for_status()
        text = resp.text
        if not text.strip():
            return label, None, "empty body"
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            return label, None, "not a YAML mapping"
        proxies = data.get("proxies")
        if not isinstance(proxies, list) or not proxies:
            return label, None, "missing or empty proxies:"
        cleaned = [p for p in proxies if isinstance(p, dict) and p.get("name") and p.get("type")]
        if not cleaned:
            return label, None, "no valid proxy objects"
        return label, cleaned, None
    except Exception as exc:  # noqa: BLE001 — per-source soft fail
        return label, None, f"{type(exc).__name__}: {exc}"


def fetch_and_merge(
    sources_file: Path,
    output: Path,
    concurrency: int = 16,
    timeout: float = 30.0,
) -> dict[str, Any]:
    urls = load_sources(sources_file)
    logger.info("Downloading %d sources (concurrency=%d)", len(urls), concurrency)

    ok = 0
    skipped = 0
    merged: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    source_counts: dict[str, int] = {}

    with httpx.Client(
        timeout=httpx.Timeout(timeout),
        follow_redirects=True,
        headers={"User-Agent": UA},
    ) as client:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(fetch_one, client, url): url for url in urls}
            for fut in as_completed(futures):
                label, proxies, err = fut.result()
                if err or proxies is None:
                    skipped += 1
                    logger.warning("SKIP %s — %s", label, err)
                    continue
                ok += 1
                kept = 0
                for proxy in proxies:
                    fp = proxy_fingerprint(proxy)
                    if fp in seen:
                        continue
                    seen.add(fp)
                    merged.append(proxy)
                    kept += 1
                source_counts[label] = kept
                logger.info("OK   %s — %d unique proxies kept", label, kept)

    output.parent.mkdir(parents=True, exist_ok=True)
    # Ensure unique names for clash-speedtest / mihomo.
    used_names: set[str] = set()
    for i, proxy in enumerate(merged):
        base = str(proxy.get("name", f"node-{i}"))
        name = base
        n = 1
        while name in used_names:
            n += 1
            name = f"{base}-{n}"
        proxy["name"] = name
        used_names.add(name)

    with output.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"proxies": merged}, fh, allow_unicode=True, sort_keys=False)

    stats = {
        "sources_total": len(urls),
        "sources_ok": ok,
        "sources_skipped": skipped,
        "proxies_merged": len(merged),
        "source_counts": source_counts,
    }
    logger.info(
        "Merge done: %d/%d sources OK, %d skipped, %d unique proxies -> %s",
        ok,
        len(urls),
        skipped,
        len(merged),
        output,
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", default="sources.txt")
    parser.add_argument("--output", default="work/merged.yaml")
    parser.add_argument("--concurrency", type=int, default=16)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    fetch_and_merge(Path(args.sources), Path(args.output), concurrency=args.concurrency)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
