"""The ``FolderProvider`` abstraction — one interface, per-transport impls
(local, smb). The detection mechanism is **event-driven** (``watch()`` yields
settled new-file events as the underlying filesystem reports them); it is NEVER
a poll/scan loop. ``list_files`` exists only for the on-demand interactive MCP
tool, never as the watch mechanism.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator
from dataclasses import dataclass


@dataclass(frozen=True)
class FileInfo:
    """A file in a root, addressed by its path relative to the root inbox."""

    relpath: str
    size: int
    mtime: float


@dataclass(frozen=True)
class SettledFile:
    """An event-driven signal that a NEW file has settled (finished being
    written) in the root inbox and is ready to ingest. ``relpath`` is relative
    to the root inbox."""

    relpath: str


class FolderProvider(abc.ABC):
    """Access boundary to one watch root. Holds the credentials/clients the
    backend must not have."""

    def __init__(self, root_name: str):
        self.root_name = root_name

    @abc.abstractmethod
    async def start(self) -> None:
        """Connect (SMB) / start the event source (local inotify). Idempotent."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Tear down the event source + any connection."""

    @abc.abstractmethod
    def watch(self) -> AsyncIterator[SettledFile]:
        """Event-driven async iterator of settled new-file events. Implementations
        MUST surface files only once settled (local: inotify CLOSE_WRITE; smb:
        CHANGE_NOTIFY + a debounce timer) and MUST NOT poll the filesystem."""

    @abc.abstractmethod
    async def stat(self, relpath: str) -> FileInfo | None:
        """Return file info, or None if the file no longer exists."""

    @abc.abstractmethod
    async def read_bytes(self, relpath: str) -> bytes:
        """Read the full file content."""

    @abc.abstractmethod
    async def move_to_subdir(self, relpath: str, subdir: str) -> str:
        """Move the file into ``subdir`` (relative to the root), EXDEV-safe (same
        filesystem). Returns the new relpath. Resolves name collisions."""

    @abc.abstractmethod
    async def list_files(self, pattern: str | None = None) -> list[FileInfo]:
        """On-demand listing of the inbox (for the interactive MCP tool only —
        NOT the watch loop). Excludes the processed/failed subdirs."""

    @property
    @abc.abstractmethod
    def connected(self) -> bool:
        """Whether the event source / connection is currently healthy."""
