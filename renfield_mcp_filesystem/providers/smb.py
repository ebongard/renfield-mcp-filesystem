"""SMB provider — event-driven via SMB2 ``CHANGE_NOTIFY`` (NEVER polling).

SMB has no CLOSE_WRITE equivalent: ``CHANGE_NOTIFY`` reports add/modify/rename
events, but not when a writer is *done*. So we debounce — a file is "settled"
once it has had no further change events for ``settle_seconds`` (an event-loop
timer per file; not a filesystem poll). The CHANGE_NOTIFY request itself blocks
server-side until a change occurs, so the watch loop is genuinely event-driven.

The settle debouncer + the change-action classification are unit-tested; the
live SMB connection, CHANGE_NOTIFY loop, and file I/O require a real share and
are exercised by the .159 E2E.
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
import ntpath
import uuid
from collections.abc import AsyncIterator, Callable

from ..config import SmbRoot
from .base import FileInfo, FolderProvider, SettledFile

logger = logging.getLogger("renfield-mcp-filesystem.smb")


def reconnect_delay(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """Exponential backoff for SMB reconnect attempts, capped."""
    return min(base * (2 ** attempt), cap)


def classify_action(action: int) -> str:
    """Map an SMB ``FileAction`` to a settle-debouncer signal: a file appearing
    or being written is a ``"change"`` (arms/re-arms settle); a file going away
    is a ``"remove"`` (cancels a pending settle); anything else is ``"ignore"``."""
    from smbprotocol.change_notify import FileAction

    if action in (
        FileAction.FILE_ACTION_ADDED,
        FileAction.FILE_ACTION_MODIFIED,
        FileAction.FILE_ACTION_RENAMED_NEW_NAME,
    ):
        return "change"
    if action in (
        FileAction.FILE_ACTION_REMOVED,
        FileAction.FILE_ACTION_REMOVED_BY_DELETE,
        FileAction.FILE_ACTION_RENAMED_OLD_NAME,
    ):
        return "remove"
    return "ignore"


class SettleDebouncer:
    """Per-file settle via event-loop timers. ``on_change`` (re)arms a timer;
    after ``settle_seconds`` of quiet for that name, ``emit(name)`` fires.
    ``on_remove`` cancels a pending timer. Timer-based — never polls."""

    def __init__(self, settle_seconds: float, emit: Callable[[str], None]):
        self._settle = settle_seconds
        self._emit = emit
        self._timers: dict[str, asyncio.TimerHandle] = {}

    def on_change(self, name: str) -> None:
        loop = asyncio.get_event_loop()
        existing = self._timers.get(name)
        if existing is not None:
            existing.cancel()
        self._timers[name] = loop.call_later(self._settle, self._fire, name)

    def on_remove(self, name: str) -> None:
        handle = self._timers.pop(name, None)
        if handle is not None:
            handle.cancel()

    def _fire(self, name: str) -> None:
        self._timers.pop(name, None)
        self._emit(name)

    def cancel_all(self) -> None:
        for handle in self._timers.values():
            handle.cancel()
        self._timers.clear()


class SmbProvider(FolderProvider):
    def __init__(self, root: SmbRoot, settle_seconds: float = 2.0):
        super().__init__(root.name)
        self._root = root
        self._inbox_unc = _unc(root.server, root.share, root.path)
        self._queue: asyncio.Queue[SettledFile] = asyncio.Queue()
        self._debouncer: SettleDebouncer | None = None
        self._settle_seconds = settle_seconds
        self._notify_task: asyncio.Task | None = None
        self._dir_open = None
        self._connected = False
        self._last_error: str | None = None
        self._reconnect_base = 2.0
        self._reconnect_cap = 60.0
        self._excluded = {root.processed_subdir, root.failed_subdir}

    @property
    def last_error(self) -> str | None:
        return self._last_error

    # -- lifecycle --

    async def connect(self) -> None:
        import smbclient

        creds = self._root.credentials()
        await asyncio.to_thread(
            smbclient.register_session,
            self._root.server, username=creds.username, password=creds.password,
            port=self._root.port,
        )
        # ensure the watched inbox + the (share-root-level) processed/failed
        # dirs exist. processed/failed are SIBLINGS of the inbox at the share
        # root (see _root_unc), not nested under it — so a share laid out as
        # \\server\share\{incomming,processed,failed} works as-is.
        await asyncio.to_thread(smbclient.makedirs, self._inbox_unc, exist_ok=True)
        for sub in (self._root.processed_subdir, self._root.failed_subdir):
            await asyncio.to_thread(smbclient.makedirs, self._root_unc(sub), exist_ok=True)

    async def start(self) -> None:
        await self.connect()
        loop = asyncio.get_running_loop()

        def _emit(name: str) -> None:
            loop.call_soon_threadsafe(self._queue.put_nowait, SettledFile(name))

        self._debouncer = SettleDebouncer(self._settle_seconds, _emit)
        self._notify_task = asyncio.create_task(self._watch_with_reconnect())
        self._connected = True

    async def stop(self) -> None:
        self._connected = False
        if self._notify_task is not None:
            self._notify_task.cancel()
            self._notify_task = None
        if self._debouncer is not None:
            self._debouncer.cancel_all()
        if self._dir_open is not None:
            try:
                await asyncio.to_thread(self._dir_open.close)
            except Exception:  # noqa: BLE001
                pass
            self._dir_open = None

    async def watch(self) -> AsyncIterator[SettledFile]:
        while True:
            yield await self._queue.get()

    @property
    def connected(self) -> bool:
        return self._connected

    # -- CHANGE_NOTIFY loop (event-driven) with reconnect/backoff (T17) --

    async def _watch_with_reconnect(self) -> None:
        """Run the CHANGE_NOTIFY loop; on a disconnect/error, notify + reconnect
        with exponential backoff (the #1 real-world SMB/NFS failure)."""
        attempt = 0
        while True:
            try:
                await self._notify_loop_once()  # runs until an error/disconnect
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._connected = False
                self._last_error = str(exc)
                logger.error("root %s: SMB watch disconnected: %s", self.root_name, exc)
                if self._disconnect_hook is not None:
                    try:
                        await self._disconnect_hook(self.root_name, str(exc))
                    except Exception:  # noqa: BLE001
                        pass
                delay = reconnect_delay(attempt, self._reconnect_base, self._reconnect_cap)
                attempt += 1
                await asyncio.sleep(delay)
                continue
            attempt = 0  # a clean return (only on shutdown) resets backoff

    async def _notify_loop_once(self) -> None:
        from smbprotocol.change_notify import CompletionFilter, FileSystemWatcher

        flags = (
            CompletionFilter.FILE_NOTIFY_CHANGE_FILE_NAME
            | CompletionFilter.FILE_NOTIFY_CHANGE_LAST_WRITE
            | CompletionFilter.FILE_NOTIFY_CHANGE_CREATION
        )
        self._dir_open = await asyncio.to_thread(self._open_inbox_dir)
        self._connected = True
        self._last_error = None
        while True:
            watcher = FileSystemWatcher(self._dir_open)
            watcher.start(flags)
            # wait() blocks until the server reports a change — event-driven.
            # ``result`` is a PROPERTY (the list of changes), not a method.
            await asyncio.to_thread(watcher.wait)
            for change in watcher.result or []:
                self._handle_change(change)

    def _handle_change(self, change) -> None:
        # change.file_name is relative to the watched dir (the inbox).
        name = getattr(change, "file_name", None) or change["file_name"].get_value()
        name = name.replace("\\", "/")
        # Only top-level inbox files; exclude the processed/failed subdirs.
        if "/" in name or name in self._excluded:
            return
        action = getattr(change, "action", None)
        if action is None:
            action = change["action"].get_value()
        kind = classify_action(int(action))
        if self._debouncer is None:
            return
        if kind == "change":
            self._debouncer.on_change(name)
        elif kind == "remove":
            self._debouncer.on_remove(name)

    def _open_inbox_dir(self):
        from smbprotocol.connection import Connection
        from smbprotocol.open import (
            CreateDisposition,
            CreateOptions,
            DirectoryAccessMask,
            FileAttributes,
            ImpersonationLevel,
            Open,
            ShareAccess,
        )
        from smbprotocol.session import Session
        from smbprotocol.tree import TreeConnect

        creds = self._root.credentials()
        conn = Connection(uuid.uuid4(), self._root.server, self._root.port)
        conn.connect()
        session = Session(conn, creds.username, creds.password)
        session.connect()
        tree = TreeConnect(session, rf"\\{self._root.server}\{self._root.share}")
        tree.connect()
        dir_rel = self._root.path.replace("/", "\\")
        dir_open = Open(tree, dir_rel)
        dir_open.create(
            ImpersonationLevel.Impersonation,
            DirectoryAccessMask.FILE_LIST_DIRECTORY | DirectoryAccessMask.SYNCHRONIZE,
            FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
            ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE | ShareAccess.FILE_SHARE_DELETE,
            CreateDisposition.FILE_OPEN,
            CreateOptions.FILE_DIRECTORY_FILE,
        )
        return dir_open

    # -- on-demand file ops (smbclient; verified at E2E) --

    def _child_unc(self, relpath: str) -> str:
        """A path WITHIN the watched inbox (`<share>/<path>/<relpath>`)."""
        return _unc(self._root.server, self._root.share, self._root.path, relpath)

    def _root_unc(self, relpath: str) -> str:
        """A path at the SHARE ROOT (`<share>/<relpath>`). The processed/failed
        subdirs live here — siblings of the inbox, not nested under it — so a
        `path: incomming` root moves files to `<share>/processed`, giving the
        `<share>/{incomming,processed,failed}` layout. With `path: ""`
        (watch the share root) this is identical to `_child_unc`."""
        return _unc(self._root.server, self._root.share, relpath)

    async def stat(self, relpath: str) -> FileInfo | None:
        import smbclient

        try:
            st = await asyncio.to_thread(smbclient.stat, self._child_unc(relpath))
        except OSError:
            return None
        return FileInfo(relpath=relpath, size=st.st_size, mtime=st.st_mtime)

    async def read_bytes(self, relpath: str) -> bytes:
        import smbclient

        def _read() -> bytes:
            with smbclient.open_file(self._child_unc(relpath), mode="rb") as fh:
                return fh.read()

        return await asyncio.to_thread(_read)

    async def move_to_subdir(self, relpath: str, subdir: str) -> str:
        import smbclient

        name = ntpath.basename(relpath.replace("/", "\\"))

        def _move() -> str:
            dest_name = name
            i = 1
            while True:
                dest_rel = f"{subdir}/{dest_name}"
                try:
                    smbclient.stat(self._root_unc(dest_rel))
                except OSError:
                    break  # free slot
                stem, dot, ext = name.partition(".")
                dest_name = f"{stem}_{i}{dot}{ext}" if dot else f"{name}_{i}"
                i += 1
            # src is in the inbox; dest is the share-root processed/failed dir.
            smbclient.rename(self._child_unc(relpath), self._root_unc(dest_rel))
            return dest_rel

        return await asyncio.to_thread(_move)

    async def list_files(self, pattern: str | None = None) -> list[FileInfo]:
        import smbclient

        def _scan() -> list[FileInfo]:
            out: list[FileInfo] = []
            for entry in smbclient.scandir(self._inbox_unc):
                if not entry.is_file():
                    continue  # excludes the processed/failed subdirs
                if pattern and not fnmatch.fnmatch(entry.name, pattern):
                    continue
                st = entry.stat()
                out.append(FileInfo(relpath=entry.name, size=st.st_size, mtime=st.st_mtime))
            return sorted(out, key=lambda fi: fi.relpath)

        return await asyncio.to_thread(_scan)


def _unc(server: str, share: str, *parts: str) -> str:
    """Build a ``\\\\server\\share\\...`` UNC path, skipping empty parts and
    normalising any forward slashes within a part to backslashes."""
    segments = [
        p.strip("/\\").replace("/", "\\") for p in parts if p and p.strip("/\\")
    ]
    tail = "\\".join(segments)
    base = rf"\\{server}\{share}"
    return f"{base}\\{tail}" if tail else base
