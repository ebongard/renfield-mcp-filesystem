"""Local-filesystem provider — event-driven via ``watchdog`` (inotify on Linux).

A file is "settled" when its writer CLOSE_WRITEs it (``FileClosedEvent``) or it
is moved into the inbox (``FileMovedEvent`` dest = IN_MOVED_TO); both mean the
write is complete. We deliberately do NOT act on bare ``on_created`` (the file
may still be open) and we NEVER scan/poll. The inbox is watched non-recursively,
so the processed/failed subdirs are naturally excluded.
"""

from __future__ import annotations

import asyncio
import fnmatch
import os
from collections.abc import AsyncIterator
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from ..config import LocalRoot
from .base import (
    FileInfo,
    FolderProvider,
    SettledFile,
    collision_candidates,
    is_bare_filename,
)


class _InboxHandler(FileSystemEventHandler):
    """Translates watchdog events into settled-file emissions. Runs on the
    watchdog thread; ``emit`` must be thread-safe (it hops to the event loop)."""

    def __init__(self, inbox: Path, excluded_names: set[str], emit):
        self._inbox = inbox
        self._excluded = excluded_names
        self._emit = emit

    def on_closed(self, event):  # inotify CLOSE_WRITE — a writer finished
        self._maybe_emit(event.src_path, event.is_directory)

    def on_moved(self, event):  # IN_MOVED_TO — a complete file moved into the inbox
        self._maybe_emit(getattr(event, "dest_path", None), event.is_directory)

    def _maybe_emit(self, path: str | None, is_directory: bool) -> None:
        if not path or is_directory:
            return
        p = Path(path)
        # Top-level inbox files only — excludes the processed/failed subdirs and
        # anything nested (we watch non-recursively, but guard anyway).
        if p.parent != self._inbox or p.name in self._excluded:
            return
        self._emit(p.name)


class LocalProvider(FolderProvider):
    def __init__(self, root: LocalRoot):
        super().__init__(root.name)
        self._inbox = Path(root.path).resolve()
        self._processed_name = root.processed_subdir
        self._failed_name = root.failed_subdir
        self._queue: asyncio.Queue[SettledFile] = asyncio.Queue()
        self._observer: Observer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connected = False

    # -- lifecycle --

    async def connect(self) -> None:
        for d in (self._inbox, self._inbox / self._processed_name, self._inbox / self._failed_name):
            d.mkdir(parents=True, exist_ok=True)

    async def start(self) -> None:
        await self.connect()
        self._loop = asyncio.get_running_loop()
        handler = _InboxHandler(
            self._inbox, {self._processed_name, self._failed_name}, self._emit_threadsafe
        )
        self._observer = Observer()
        self._observer.schedule(handler, str(self._inbox), recursive=False)
        self._observer.start()
        self._connected = True

    def _emit_threadsafe(self, name: str) -> None:
        # Called from the watchdog thread → hop to the loop thread.
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, SettledFile(name))

    async def stop(self) -> None:
        self._connected = False
        if self._observer is not None:
            self._observer.stop()
            await asyncio.to_thread(self._observer.join, 5)
            self._observer = None

    async def watch(self) -> AsyncIterator[SettledFile]:
        while True:
            yield await self._queue.get()

    @property
    def connected(self) -> bool:
        return self._connected and (self._observer is None or self._observer.is_alive())

    # -- path safety --

    def _resolve(self, relpath: str) -> Path:
        """Join + resolve a relpath, rejecting traversal outside the inbox."""
        p = (self._inbox / relpath).resolve()
        if p != self._inbox and self._inbox not in p.parents:
            raise ValueError(f"path traversal rejected: {relpath!r}")
        return p

    # -- on-demand ops --

    async def stat(self, relpath: str) -> FileInfo | None:
        p = self._resolve(relpath)
        try:
            st = await asyncio.to_thread(p.stat)
        except FileNotFoundError:
            return None
        if not p.is_file():
            return None
        return FileInfo(relpath=relpath, size=st.st_size, mtime=st.st_mtime)

    async def read_bytes(self, relpath: str) -> bytes:
        p = self._resolve(relpath)
        return await asyncio.to_thread(p.read_bytes)

    async def move_to_subdir(self, relpath: str, subdir: str) -> str:
        src = self._resolve(relpath)
        destdir = self._inbox / subdir
        await asyncio.to_thread(lambda: destdir.mkdir(parents=True, exist_ok=True))
        dest = self._unique_dest(destdir, src.name)
        # os.replace is atomic within a filesystem; the subdir is inside the root
        # so this is EXDEV-safe by construction.
        await asyncio.to_thread(os.replace, src, dest)
        return str(dest.relative_to(self._inbox))

    @staticmethod
    def _unique_dest(destdir: Path, name: str) -> Path:
        dest = destdir / name
        if not dest.exists():
            return dest
        stem, dot, ext = name.partition(".")
        i = 1
        while True:
            candidate = destdir / (f"{stem}_{i}{dot}{ext}" if dot else f"{name}_{i}")
            if not candidate.exists():
                return candidate
            i += 1

    async def rename_within_processed(self, src_name: str, target_name: str) -> str | None:
        if not is_bare_filename(src_name) or not is_bare_filename(target_name):
            raise ValueError("rename names must be bare filenames")

        def _do() -> str | None:
            procdir = self._inbox / self._processed_name
            src = procdir / src_name
            if not src.is_file():
                return None  # idempotent no-op: already renamed / never moved here
            for cand in collision_candidates(target_name):
                dest = procdir / cand
                if dest == src:
                    return str(src.relative_to(self._inbox))  # already the target name
                if not dest.exists():
                    os.replace(src, dest)  # atomic within the same filesystem
                    return str(dest.relative_to(self._inbox))
            return None  # unreachable (collision_candidates is infinite)

        return await asyncio.to_thread(_do)

    async def list_files(self, pattern: str | None = None) -> list[FileInfo]:
        def _scan() -> list[FileInfo]:
            out: list[FileInfo] = []
            for entry in os.scandir(self._inbox):
                if not entry.is_file():
                    continue  # excludes the processed/failed subdirs
                if pattern and not fnmatch.fnmatch(entry.name, pattern):
                    continue
                st = entry.stat()
                out.append(FileInfo(relpath=entry.name, size=st.st_size, mtime=st.st_mtime))
            return sorted(out, key=lambda fi: fi.relpath)

        return await asyncio.to_thread(_scan)
