"""The local accept/reject gate, shared by the engine (before any push) and the
dry-run (to report matched-vs-skipped). One definition so they never diverge."""

from __future__ import annotations

from .config import Config


def classify(config: Config, filename: str, size: int) -> tuple[bool, str | None]:
    """Return ``(accepted, reason)``. A rejected file is terminal (it won't
    become valid) → the engine moves it to ``failed/`` without pushing; dry-run
    lists it as skipped-and-why."""
    if size == 0:
        return False, "empty"
    if size > config.max_file_size_bytes:
        return False, "oversize"
    if not config.extension_allowed(filename):
        return False, "extension_not_allowed"
    return True, None
