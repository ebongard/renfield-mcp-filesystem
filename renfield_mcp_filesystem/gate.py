"""The local accept/reject gate, shared by the engine (before any push) and the
dry-run (to report matched-vs-skipped). One definition so they never diverge."""

from __future__ import annotations

from .config import Config


def classify(config: Config, filename: str, size: int) -> tuple[bool, str | None]:
    """Return ``(accepted, reason)``. A rejected file is terminal (it won't
    become valid) → the engine moves it to ``failed/`` without pushing; dry-run
    lists it as skipped-and-why.

    Exception: the ``"empty"`` (0-byte) reason is NOT treated as terminal by the
    engine — a 0-byte read at settle time is usually a mid-copy read on
    CLOSE_WRITE-less SMB, so the engine retries it a few times (then fails only if
    it stays empty). Dry-run still reports it as skipped-empty."""
    if size == 0:
        return False, "empty"
    if size > config.max_file_size_bytes:
        return False, "oversize"
    if not config.extension_allowed(filename):
        return False, "extension_not_allowed"
    return True, None
