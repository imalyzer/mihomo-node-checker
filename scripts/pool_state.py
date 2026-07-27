#!/usr/bin/env python3
"""Sticky pool state: fingerprints, streaks, and stable/fresh splitting."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fetch_and_merge import proxy_fingerprint
from sanitize_proxies import strip_internal

logger = logging.getLogger(__name__)

STABLE_STREAK = 3
STABLE_CAP = 80
FRESH_CAP = 120

LIGHT_TARGETS = [
    "api.openai.com",
    "api.anthropic.com",
    "api.cursor.sh",
    "aistudio.google.com",
]


def fp_key(proxy: dict[str, Any]) -> str:
    return "|".join(proxy_fingerprint(proxy))


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"updated_at": None, "nodes": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to load pool state %s: %s", path, exc)
        return {"updated_at": None, "nodes": {}}
    if not isinstance(data, dict):
        return {"updated_at": None, "nodes": {}}
    nodes = data.get("nodes")
    if not isinstance(nodes, dict):
        nodes = {}
    return {"updated_at": data.get("updated_at"), "nodes": nodes}


def save_state(path: Path, nodes: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": utc_now(), "nodes": nodes}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved pool state (%d nodes) -> %s", len(nodes), path)


def proxies_from_state(state: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entry in (state.get("nodes") or {}).values():
        if not isinstance(entry, dict):
            continue
        proxy = entry.get("proxy")
        if isinstance(proxy, dict) and proxy.get("name") and proxy.get("type"):
            out.append(dict(proxy))
    return out


def load_proxies_yaml(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = yaml_safe_load(path)
    proxies = data.get("proxies") if isinstance(data, dict) else None
    if not isinstance(proxies, list):
        return []
    return [p for p in proxies if isinstance(p, dict)]


def yaml_safe_load(path: Path) -> Any:
    import yaml

    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def bootstrap_state_from_outputs(
    state_path: Path,
    backup_path: Path,
    stable_path: Path,
    fresh_path: Path,
) -> dict[str, Any]:
    """Load state file, or reconstruct from previous output YAMLs on first sticky run."""
    state = load_state(state_path)
    if state.get("nodes"):
        return state

    # Prefer union of stable+fresh+backup if present.
    seen: dict[str, dict[str, Any]] = {}
    for path, default_streak in (
        (stable_path, STABLE_STREAK),
        (fresh_path, 1),
        (backup_path, 1),
    ):
        for proxy in load_proxies_yaml(path):
            key = fp_key(proxy)
            if key in seen:
                continue
            clean = strip_internal(proxy)
            seen[key] = {
                "streak": default_streak if path == stable_path else 1,
                "last_ok_at": utc_now(),
                "proxy": clean,
            }
    if seen:
        logger.info("Bootstrapped pool state from outputs (%d nodes)", len(seen))
        return {"updated_at": utc_now(), "nodes": seen}
    return state


def split_newcomers(
    candidates: list[dict[str, Any]],
    pool_fps: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (already_in_pool, newcomers)."""
    known: list[dict[str, Any]] = []
    newcomers: list[dict[str, Any]] = []
    for proxy in candidates:
        key = fp_key(proxy)
        if key in pool_fps:
            known.append(proxy)
        else:
            newcomers.append(proxy)
    return known, newcomers


def ensure_unique_names(proxies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    used: set[str] = set()
    out: list[dict[str, Any]] = []
    for i, proxy in enumerate(proxies):
        p = dict(proxy)
        base = str(p.get("name") or f"node-{i}")
        name = base
        n = 1
        while name in used:
            n += 1
            name = f"{base}-{n}"
        p["name"] = name
        used.add(name)
        out.append(p)
    return out


def partition_stable_fresh(
    nodes: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (stable, fresh, union) capped; priority by streak desc then last_ok desc."""
    items = list(nodes.items())
    items.sort(
        key=lambda kv: (
            int(kv[1].get("streak") or 0),
            str(kv[1].get("last_ok_at") or ""),
        ),
        reverse=True,
    )

    stable: list[dict[str, Any]] = []
    fresh: list[dict[str, Any]] = []
    for _key, entry in items:
        proxy = entry.get("proxy")
        if not isinstance(proxy, dict):
            continue
        streak = int(entry.get("streak") or 0)
        if streak >= STABLE_STREAK:
            if len(stable) < STABLE_CAP:
                stable.append(dict(proxy))
            elif len(fresh) < FRESH_CAP:
                fresh.append(dict(proxy))
        else:
            if len(fresh) < FRESH_CAP:
                fresh.append(dict(proxy))

    stable = ensure_unique_names(stable)
    stable_names = {p["name"] for p in stable}
    renamed_fresh: list[dict[str, Any]] = []
    for p in fresh:
        q = dict(p)
        if q["name"] in stable_names:
            q["name"] = f"{q['name']} [fresh]"
        renamed_fresh.append(q)
    fresh = ensure_unique_names(renamed_fresh)

    union_map: dict[str, dict[str, Any]] = {}
    for p in stable + fresh:
        union_map[fp_key(p)] = p
    union = list(union_map.values())
    return stable, fresh, union
