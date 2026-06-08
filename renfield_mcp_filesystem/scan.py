"""Dry-run (T16): resolve every root, list what WOULD be pushed vs what's
skipped-and-why — moving nothing and pushing nothing. Validate config (roots,
credentials, extension/size limits) before the daemon touches real documents.

    python -m renfield_mcp_filesystem.scan --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass, field

from .config import Config, load_config
from .gate import classify
from .registry import make_provider

logger = logging.getLogger("renfield-mcp-filesystem.scan")


@dataclass
class RootReport:
    name: str
    type: str
    error: str | None = None
    matched: list[tuple[str, int]] = field(default_factory=list)  # (relpath, size)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (relpath, reason)


async def dry_run(config: Config) -> list[RootReport]:
    """Enumerate + classify each root. Never pushes, never moves. Connects (SMB
    session / local dirs) read-only; a connect/list failure is reported per root
    so a bad credential or unreachable share surfaces here, not at runtime."""
    reports: list[RootReport] = []
    for root in config.roots:
        report = RootReport(name=root.name, type=root.type)
        try:
            provider = make_provider(root, config.settle_seconds)
            await provider.connect()
            files = await provider.list_files()
        except Exception as exc:  # noqa: BLE001
            report.error = str(exc)
            reports.append(report)
            continue
        for fi in files:
            ok, reason = classify(config, fi.relpath, fi.size)
            if ok:
                report.matched.append((fi.relpath, fi.size))
            else:
                report.skipped.append((fi.relpath, reason or "rejected"))
        reports.append(report)
    return reports


def format_report(reports: list[RootReport]) -> str:
    lines: list[str] = ["DRY RUN — nothing was pushed or moved.\n"]
    for r in reports:
        lines.append(f"root {r.name} ({r.type}):")
        if r.error:
            lines.append(f"  ERROR: {r.error}")
            continue
        if r.matched:
            lines.append(f"  would push ({len(r.matched)}):")
            for relpath, size in r.matched:
                lines.append(f"    + {relpath} ({size} bytes)")
        if r.skipped:
            lines.append(f"  skipped ({len(r.skipped)}):")
            for relpath, reason in r.skipped:
                lines.append(f"    - {relpath} ({reason})")
        if not r.matched and not r.skipped:
            lines.append("  (inbox empty)")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="renfield-mcp-filesystem scanner")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="list matched vs skipped per root; push/move nothing (the only mode)",
    )
    parser.parse_args(argv)
    logging.basicConfig(level="WARNING", stream=sys.stderr)

    config = load_config()
    reports = asyncio.run(dry_run(config))
    print(format_report(reports))
    # Non-zero exit if any root errored, so CI / a preflight can gate on it.
    return 1 if any(r.error for r in reports) else 0


if __name__ == "__main__":
    raise SystemExit(main())
