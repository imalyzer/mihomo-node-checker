#!/usr/bin/env python3
"""Download free Clash/Mihomo sources, merge proxies, and strong-dedupe.

MihomoSaz Sublist files are often full configs whose nodes live under
proxy-providers.*.url — those nested URLs are followed automatically.
"""

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


def _normalize_proxies(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict) and isinstance(raw.get("proxies"), list):
        items = raw["proxies"]
    else:
        return []
    return [p for p in items if isinstance(p, dict) and p.get("name") and p.get("type") and p.get("server")]


def _provider_urls(data: dict[str, Any]) -> list[str]:
    providers = data.get("proxy-providers")
    if not isinstance(providers, dict):
        return []
    urls: list[str] = []
    for _name, cfg in providers.items():
        if not isinstance(cfg, dict):
            continue
        if str(cfg.get("type", "http")).lower() not in {"http", "https", ""}:
            continue
        url = cfg.get("url")
        if isinstance(url, str):
            url = url.strip()
            if url.startswith("http://") or url.startswith("https://"):
                urls.append(url)
    return urls


def _load_yaml(text: str) -> Any:
    return yaml.safe_load(text)


def _fetch_proxies_from_url(client: httpx.Client, url: str, depth: int = 0) -> list[dict[str, Any]]:
    """Fetch a URL and return proxy objects, optionally following one provider hop."""
    resp = client.get(url)
    resp.raise_for_status()
    text = resp.text
    if not text.strip():
        return []

    try:
        data = _load_yaml(text)
    except Exception:  # noqa: BLE001
        return []

    proxies = _normalize_proxies(data)
    if proxies:
        return proxies

    if depth >= 1 or not isinstance(data, dict):
        return []

    nested: list[dict[str, Any]] = []
    for nested_url in _provider_urls(data):
        try:
            nested.extend(_fetch_proxies_from_url(client, nested_url, depth=depth + 1))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Nested provider fail %s — %s", _source_label(nested_url), exc)
    return nested


def fetch_one(client: httpx.Client, url: str) -> tuple[str, list[dict[str, Any]] | None, str | None]:
    label = _source_label(url)
    try:
        proxies = _fetch_proxies_from_url(client, url, depth=0)
        if not proxies:
            return label, None, "no proxies (inline or via proxy-providers)"
        return label, proxies, None
    except Exception as exc:  # noqa: BLE001 — per-source soft fail
        return label, None, f"{type(exc).__name__}: {exc}"


def fetch_and_merge(
    sources_file: Path,
    output: Path,
    concurrency: int = 16,
    timeout: float = 45.0,
) -> dict[str, Any]:
    urls = load_sources(sources_file)
    logger.info("Downloading %d sources (concurrency=%d)", len(urls), concurrency)

    ok = 0
    skipped = 0
    merged: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    source_counts: dict[str, int] = {}

    # One client per worker thread keeps httpx usage simple under a pool.
    def _worker(source_url: str) -> tuple[str, list[dict[str, Any]] | None, str | None]:
        with httpx.Client(
            timeout=httpx.Timeout(timeout),
            follow_redirects=True,
            headers={"User-Agent": UA},
        ) as client:
            return fetch_one(client, source_url)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_worker, url): url for url in urls}
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
                # Keep source label for sanitize logs only; stripped before speedtest.
                annotated = dict(proxy)
                annotated["_source"] = label
                merged.append(annotated)
                kept += 1
            source_counts[label] = kept
            logger.info("OK   %s — %d unique proxies kept", label, kept)

    output.parent.mkdir(parents=True, exist_ok=True)
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
