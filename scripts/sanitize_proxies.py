#!/usr/bin/env python3
"""Lightweight validation of merged Clash/Mihomo proxy objects before speedtest."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

SHORT_ID_RE = re.compile(r"^[0-9a-fA-F]{0,16}$")
# x25519 public keys in Clash are typically URL-safe base64 (~43-44 chars).
PUBLIC_KEY_RE = re.compile(r"^[A-Za-z0-9+/_-]{40,50}={0,2}$")

INTERNAL_KEYS = ("_source",)


def _port_ok(port: Any) -> bool:
    try:
        p = int(port)
    except (TypeError, ValueError):
        return False
    return 1 <= p <= 65535


def validate_proxy(proxy: dict[str, Any]) -> str | None:
    """Return rejection reason, or None if the proxy looks loadable."""
    if not isinstance(proxy, dict):
        return "not a mapping"

    name = proxy.get("name")
    if not name:
        return "missing name"

    ptype = str(proxy.get("type") or "").lower().strip()
    if not ptype:
        return "missing type"

    server = proxy.get("server")
    if not server or not str(server).strip():
        return "missing/empty server"

    if not _port_ok(proxy.get("port")):
        return f"invalid port: {proxy.get('port')!r}"

    if ptype in {"vless", "vmess"}:
        uuid = proxy.get("uuid")
        if not uuid or not str(uuid).strip():
            return f"{ptype} missing uuid"

    if ptype in {"ss", "shadowsocks", "trojan", "ssr", "hysteria", "hysteria2", "anytls"}:
        # hysteria2 may use password or auth; require at least one identity field.
        if ptype in {"hysteria", "hysteria2"}:
            if not (proxy.get("password") or proxy.get("auth")):
                return f"{ptype} missing password/auth"
        elif not proxy.get("password"):
            return f"{ptype} missing password"

    reality = proxy.get("reality-opts")
    if isinstance(reality, dict) and reality:
        short_id = reality.get("short-id", "")
        if short_id is None:
            short_id = ""
        short_id = str(short_id)
        if short_id and (len(short_id) % 2 != 0 or not SHORT_ID_RE.fullmatch(short_id)):
            return f"invalid reality short-id: {short_id!r}"

        public_key = reality.get("public-key")
        if not public_key or not str(public_key).strip():
            return "reality missing public-key"
        pk = str(public_key).strip()
        # x25519 base64 is typically 43–44 chars; allow tiny padding variance.
        if not (43 <= len(pk) <= 44) or not PUBLIC_KEY_RE.fullmatch(pk):
            return f"invalid reality public-key length/format: len={len(pk)}"

    return None


def strip_internal(proxy: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in proxy.items() if k not in INTERNAL_KEYS}


def sanitize_proxies(proxies: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    rejected = 0
    reasons: dict[str, int] = {}

    for proxy in proxies:
        if not isinstance(proxy, dict):
            rejected += 1
            reasons["not a mapping"] = reasons.get("not a mapping", 0) + 1
            logger.warning("REJECT <unknown> source=- reason=not a mapping")
            continue
        reason = validate_proxy(proxy)
        if reason:
            rejected += 1
            reasons[reason] = reasons.get(reason, 0) + 1
            logger.warning(
                "REJECT name=%r source=%s reason=%s",
                proxy.get("name"),
                proxy.get("_source", "-"),
                reason,
            )
            continue
        kept.append(strip_internal(proxy))

    stats = {
        "input": len(proxies),
        "kept": len(kept),
        "rejected": rejected,
        "reasons": reasons,
    }
    logger.info(
        "Sanitize: kept %d/%d proxies (%d rejected)",
        len(kept),
        len(proxies),
        rejected,
    )
    return kept, stats


def sanitize_file(input_path: Path, output_path: Path) -> dict[str, Any]:
    data = yaml.safe_load(input_path.read_text(encoding="utf-8")) or {}
    proxies = data.get("proxies") if isinstance(data, dict) else None
    if not isinstance(proxies, list):
        proxies = []
    kept, stats = sanitize_proxies(proxies)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"proxies": kept}, fh, allow_unicode=True, sort_keys=False)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="work/merged.yaml")
    parser.add_argument("--output", default="work/sanitized.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sanitize_file(Path(args.input), Path(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
