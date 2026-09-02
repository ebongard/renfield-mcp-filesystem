"""The ``FolderProvider`` abstraction — one interface, per-transport impls
(local, smb). The detection mechanism is **event-driven** (``watch()`` yields
settled new-file events as the underlying filesystem reports them); it is NEVER
a poll/scan loop. ``list_files`` exists only for the on-demand interactive MCP
tool, never as the watch mechanism.
"""

from __future__ import annotations

import abc
import re
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

# Characters illegal in SMB/Windows filenames, plus the path separators. Replaced
# with a single space (later collapsed). Control chars are stripped separately.
_ILLEGAL_SMB_CHARS = r'/\\:*?"<>|'
_ILLEGAL_RE = re.compile(f"[{re.escape(_ILLEGAL_SMB_CHARS)}]")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_WS_RE = re.compile(r"\s+")

# Cap the base name so ``<base><ext>`` stays comfortably within filesystem limits.
_MAX_BASE_LEN = 150


def sanitize_filename_base(base: str, *, max_len: int = _MAX_BASE_LEN) -> str:
    """Scrub ``base`` into a safe SMB/Windows filename component (NO extension).

    Defense-in-depth mirror of the backend's ``sanitize_smb_filename`` (#881):
    strips/replaces ``/ \\ : * ? " < > |`` and control chars, collapses
    whitespace, trims leading/trailing whitespace and dots (Windows forbids
    trailing dots/spaces), and caps the length. Returns ``""`` if nothing usable
    remains.
    """
    if not base:
        return ""
    s = _CONTROL_RE.sub("", str(base))
    s = _ILLEGAL_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    s = s.strip(" .")
    if len(s) > max_len:
        s = s[:max_len].strip(" .")
    return s


def split_ext(name: str) -> tuple[str, str]:
    """Split a filename into ``(stem, ext)`` where ``ext`` includes the leading
    dot (or is ``""``). Keeps compound names intact (``a.b.pdf`` → ``a.b``, ``.pdf``);
    a leading-dot dotfile with no other dot has no extension."""
    stem, dot, ext = name.rpartition(".")
    if not dot or not stem:  # "noext" or ".hidden" → no real extension
        return name, ""
    return stem, "." + ext


def collision_candidates(name: str) -> Iterator[str]:
    """Yield ``name`` then ``<stem> (2)<ext>``, ``<stem> (3)<ext>``, … for
    collision-safe renames (#881). Infinite generator — the caller stops at the
    first free slot."""
    yield name
    stem, ext = split_ext(name)
    i = 2
    while True:
        yield f"{stem} ({i}){ext}"
        i += 1


def is_bare_filename(name: str) -> bool:
    """True iff ``name`` is a single safe filename component — no path separators,
    no drive/ADS colon, no ``..`` traversal, not ``.``/``..`` itself. Guards the
    rename entrypoints so a crafted ``original_name``/target can't escape the
    processed/ dir."""
    if not name or not name.strip():
        return False
    if "/" in name or "\\" in name or ":" in name:
        return False
    if name in (".", ".."):
        return False
    return True


def safe_relpath(relpath: str) -> str:
    """Validate a root-relative path, rejecting anything that could escape the
    watch root. Raises ``ValueError`` on:

    - empty / whitespace-only paths,
    - absolute paths (leading ``/`` or ``\\``),
    - drive letters / UNC prefixes / NTFS alternate-data-streams (any ``:``),
    - ``..`` traversal in any segment (``/`` or ``\\`` separated).

    Security (review H5): the ``LocalProvider`` self-guards via ``_resolve``
    (realpath + commonpath), but the ``SmbProvider`` built UNC paths straight
    from the relpath, preserving ``..`` for the SMB server to resolve — an
    arbitrary-file-read escape from the inbox. This is the shared choke-point
    guard, applied both at the tool boundary (``split_path``) and inside the
    SMB provider as defense in depth. Returns the stripped relpath on success.
    """
    rp = (relpath or "").strip()
    if not rp:
        raise ValueError("empty relpath")
    if rp.startswith(("/", "\\")):
        raise ValueError(f"absolute path rejected: {relpath!r}")
    if ":" in rp:
        raise ValueError(f"path traversal rejected: {relpath!r}")
    segments = rp.replace("\\", "/").split("/")
    if any(seg == ".." for seg in segments):
        raise ValueError(f"path traversal rejected: {relpath!r}")
    return rp


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
        self._disconnect_hook = None

    def set_disconnect_hook(self, cb) -> None:
        """Register ``async cb(root_name, reason)`` called when the provider
        loses its connection (before it reconnects). Used for operator notify."""
        self._disconnect_hook = cb

    @property
    def last_error(self) -> str | None:
        """The most recent connection/watch error, or None if healthy."""
        return None

    async def connect(self) -> None:
        """Establish I/O readiness (SMB session / local dirs) WITHOUT starting
        the watch. Used by dry-run to list + classify without side effects.
        ``start()`` calls this first. Default: no-op."""

    @abc.abstractmethod
    async def start(self) -> None:
        """connect() + start the event source (local inotify / SMB CHANGE_NOTIFY).
        Idempotent."""

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
    async def rename_within_processed(
        self, src_name: str, target_name: str
    ) -> str | None:
        """Rename ``processed/<src_name>`` → ``processed/<target_name>`` (#881).

        - **Idempotent:** returns ``None`` (a no-op success) when ``src_name`` is
          not present in ``processed/`` — already renamed, or never moved there.
        - **Collision-safe:** if the target name is taken by a DIFFERENT file,
          appends `` (2)``, `` (3)``, … until free.
        - When ``src_name == target_name`` (or the only collision is the source
          itself) it is a no-op that returns the current relpath.

        Both names are bare filename components (validated at the tool boundary).
        Returns the new relpath (relative to the root inbox), or ``None`` no-op.
        """

    @abc.abstractmethod
    async def list_files(self, pattern: str | None = None) -> list[FileInfo]:
        """On-demand listing of the inbox (for the interactive MCP tool only —
        NOT the watch loop). Excludes the processed/failed subdirs."""

    @property
    @abc.abstractmethod
    def connected(self) -> bool:
        """Whether the event source / connection is currently healthy."""
