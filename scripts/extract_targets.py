#!/usr/bin/env python3
"""Extract representative Group-A domains from a local Master-Config.yaml."""

from __future__ import annotations

import argparse
import logging
import re
from collections import defaultdict
from pathlib import Path

SECTION_RE = re.compile(r"^\s*#\s*---\s*(.+?)\s*---\s*$")
RULE_RE = re.compile(
    r"^\s*-\s*(DOMAIN|DOMAIN-SUFFIX|DOMAIN-KEYWORD),([^,]+),Group-A-Traffic\s*$"
)
LOW_PRIORITY_PREFIXES = (
    "cdn.",
    "static.",
    "assets.",
    "telemetry.",
    "stats.",
    "media.",
    "images.",
    "log.",
)

logger = logging.getLogger(__name__)


def _is_low_priority(host: str) -> bool:
    h = host.lower()
    return any(h.startswith(p) for p in LOW_PRIORITY_PREFIXES)


def _score(rule_type: str, host: str) -> tuple[int, int, int, int]:
    """Lower score = better candidate."""
    if rule_type == "DOMAIN-KEYWORD":
        return (99, 99, 99, 99)
    h = host.lower()
    low = 1 if _is_low_priority(h) else 0
    api_like = (
        h.startswith("api.")
        or h.startswith("api2.")
        or h.startswith("api3.")
        or "googleapis.com" in h
        or h.endswith(".openai.com")
        or h in {"aistudio.google.com", "claude.ai", "chatgpt.com", "cursor.sh", "x.ai"}
    )
    if rule_type == "DOMAIN":
        kind = 0 if api_like else 1
    elif rule_type == "DOMAIN-SUFFIX":
        # Prefer product suffixes slightly over obscure full DOMAINs.
        kind = 0 if api_like else 2
    else:
        kind = 3
    # Prefer fewer labels (less nested) then shorter.
    return (low, kind, h.count("."), len(h))


def extract_sections(config_text: str) -> dict[str, list[tuple[str, str]]]:
    """Return section -> list of (rule_type, host) for Group-A rules."""
    in_rules = False
    section = "misc"
    sections: dict[str, list[tuple[str, str]]] = defaultdict(list)
    total = 0

    for line in config_text.splitlines():
        if not in_rules:
            if re.match(r"^rules:\s*$", line):
                in_rules = True
            continue

        # End of rules block if we hit a top-level key (unlikely after rules, but safe).
        if re.match(r"^[a-zA-Z0-9_-]+:\s*$", line) and not line.startswith(" "):
            break

        sec_m = SECTION_RE.match(line)
        if sec_m:
            section = sec_m.group(1).strip() or "misc"
            continue

        rule_m = RULE_RE.match(line)
        if not rule_m:
            continue
        rule_type, host = rule_m.group(1), rule_m.group(2).strip()
        total += 1
        if rule_type == "DOMAIN-KEYWORD":
            continue
        sections[section].append((rule_type, host))

    logger.info("Found %d Group-A-Traffic rules across %d sections", total, len(sections))
    return sections


def sample_targets(sections: dict[str, list[tuple[str, str]]], per_section: int = 2) -> list[str]:
    chosen: list[str] = []
    seen: set[str] = set()

    for section, rules in sections.items():
        ranked = sorted(rules, key=lambda r: _score(r[0], r[1]))
        picked = 0
        for rule_type, host in ranked:
            if _score(rule_type, host)[0] == 99:
                continue
            # Skip low-priority unless nothing else left in section.
            if _is_low_priority(host):
                continue
            key = host.lower()
            if key in seen:
                continue
            seen.add(key)
            chosen.append(host)
            picked += 1
            if picked >= per_section:
                break

        # Fallback: allow low-priority if section still empty.
        if picked == 0:
            for rule_type, host in ranked:
                if rule_type == "DOMAIN-KEYWORD":
                    continue
                key = host.lower()
                if key in seen:
                    continue
                seen.add(key)
                chosen.append(host)
                picked += 1
                if picked >= per_section:
                    break

        logger.info("Section %-20s -> %s", section, chosen[-picked:] if picked else [])

    return chosen


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Path to Master-Config.yaml")
    parser.add_argument(
        "--output",
        default="data/group-a-targets.txt",
        help="Output targets file",
    )
    parser.add_argument("--per-section", type=int, default=2)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    config_path = Path(args.config)
    text = config_path.read_text(encoding="utf-8")
    sections = extract_sections(text)
    targets = sample_targets(sections, per_section=args.per_section)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(targets) + ("\n" if targets else ""), encoding="utf-8")
    logger.info("Wrote %d sampled targets to %s", len(targets), out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
