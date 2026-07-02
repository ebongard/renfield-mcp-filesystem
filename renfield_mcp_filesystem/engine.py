"""Per-root ingest engine: consumes the provider's event-driven ``watch()`` and
drives each settled file through gate → hash → push → move.

Detection is event-driven (NEVER a poll loop). Two non-poll complements:
  1. **One-shot startup reconciliation** — inotify only reports events that occur
     AFTER the watch starts, so files already in the inbox at boot would be
     invisible forever. We enumerate the inbox ONCE at startup and dispatch those
     files, then rely purely on events. This is a single catch-up, not a periodic
     scan.
  2. **Bounded per-file retry** — a ``retry`` response (worker down / in-flight)
     leaves the file in place and re-attempts THAT file on an exponential backoff
     (a retry schedule for one known unit of work, not a filesystem poll). After
     the cap we give up and leave it (the next restart's reconciliation re-tries).

Local size + extension gating happens HERE, before any push (security: never
upload an oversized / disallowed file — the backend would reject it anyway).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import os
from collections.abc import Awaitable, Callable

from .config import Config, Root
from .contract import MoveAction
from .gate import classify
from .providers.base import FolderProvider
from .pusher import PushOutcome, RenfieldPusher

logger = logging.getLogger("renfield-mcp-filesystem.engine")

# Operator-notification callback: (root_name, relpath, reason) -> awaitable.
FailureHook = Callable[[str, str, str], Awaitable[None]]
# Fatal (token/config) callback: (root_name, reason) -> awaitable.
FatalHook = Callable[[str, str], Awaitable[None]]


class IngestEngine:
    def __init__(
        self,
        *,
        config: Config,
        root: Root,
        provider: FolderProvider,
        pusher: RenfieldPusher,
        on_failed: FailureHook | None = None,
        on_fatal: FatalHook | None = None,
        max_retries: int = 5,
        retry_base_seconds: float = 2.0,
        push_semaphore: asyncio.Semaphore | None = None,
    ):
        self._config = config
        self._root = root
        self._provider = provider
        self._pusher = pusher
        self._on_failed = on_failed
        self._on_fatal = on_fatal
        self._max_retries = max_retries
        self._retry_base = retry_base_seconds
        # Shared across all roots + retries (built by the daemon) so a burst can
        # never fan out into a flood of simultaneous pushes. A no-op unbounded
        # fallback keeps the engine usable standalone (tests).
        self._push_sem = push_semaphore or asyncio.Semaphore(2**31)
        self._inflight: set[str] = set()
        self._retry_tasks: set[asyncio.Task] = set()
        self._stopped = asyncio.Event()

    async def run(self) -> None:
        await self._provider.start()
        logger.info("root %s: started; reconciling existing files", self._root.name)
        await self._reconcile_existing()
        async for event in self._provider.watch():
            if self._stopped.is_set():
                break
            await self._dispatch(event.relpath)

    async def stop(self) -> None:
        self._stopped.set()
        for t in list(self._retry_tasks):
            t.cancel()
        await self._provider.stop()

    async def reconcile(self) -> None:
        """Re-run the one-shot inbox catch-up on demand — the daemon calls this
        when the backend recovers (down→up), so files left in the inbox after
        retry-exhaustion during the outage are re-attempted WITHOUT a restart.
        A no-op on a stopped root (a fatal token error needs operator action)."""
        if self._stopped.is_set():
            return
        await self._reconcile_existing()

    async def _reconcile_existing(self) -> None:
        """One-shot startup catch-up (NOT a poll). See module docstring."""
        try:
            existing = await self._provider.list_files()
        except Exception as exc:  # noqa: BLE001 - reconciliation is best-effort
            logger.warning("root %s: startup reconciliation failed: %s", self._root.name, exc)
            return
        for fi in existing:
            await self._dispatch(fi.relpath)

    async def _dispatch(self, relpath: str) -> None:
        # CRITICAL: an empty / whitespace / slash-only relpath resolves to the
        # inbox directory ITSELF — moving it would relocate the entire inbox
        # into failed/. A provider must never hand one through (a directory-level
        # CHANGE_NOTIFY with no file name can produce ""), so guard here as the
        # provider-agnostic safety net before anything touches the filesystem.
        if not relpath.strip().strip("/\\"):
            logger.warning("root %s: ignoring empty/dir-level event", self._root.name)
            return
        # Ignore dotfiles (.DS_Store, .hidden, editor temp/lock files). They are
        # noise, and OS-created ones (e.g. macOS .DS_Store) are often permission-
        # locked so even moving them to failed/ errors.
        if os.path.basename(relpath).startswith("."):
            return
        # Create-only / de-dup: ignore repeat events for a file already being
        # processed or awaiting retry (e.g. multiple CLOSE_WRITE for one write).
        if relpath in self._inflight:
            return
        self._inflight.add(relpath)
        await self._process(relpath, attempt=0)

    async def _process(self, relpath: str, attempt: int) -> None:
        try:
            info = await self._provider.stat(relpath)
            if info is None:
                self._inflight.discard(relpath)  # already moved / vanished
                return
            ok, reason = classify(self._config, relpath, info.size)
            if not ok:
                await self._move(relpath, MoveAction.FAILED, reason or "rejected")
                return

            data = await self._provider.read_bytes(relpath)
            sha = hashlib.sha256(data).hexdigest()
            mime = mimetypes.guess_type(relpath)[0]
            # Bound concurrent pushes across all roots + retries (defense-in-depth
            # against a backlog/retry-storm flooding the backend). Held only for
            # the network round-trip, not the local read/hash above.
            async with self._push_sem:
                outcome = await self._pusher.push(
                    file_bytes=data,
                    filename=os.path.basename(relpath),
                    root=self._root.name,
                    relpath=relpath,
                    sha256=sha,
                    mime=mime,
                )
            await self._apply_outcome(relpath, attempt, outcome)
        except Exception as exc:  # noqa: BLE001 - never drop a file on a bug
            logger.exception("root %s: error processing %s: %s", self._root.name, relpath, exc)
            await self._schedule_retry(relpath, attempt, "processing_error")

    async def _apply_outcome(self, relpath: str, attempt: int, outcome: PushOutcome) -> None:
        if outcome.fatal:
            self._inflight.discard(relpath)
            self._stopped.set()  # token/config error affects the whole root
            logger.error("root %s: fatal push outcome — stopping root", self._root.name)
            if self._on_fatal is not None:
                await self._on_fatal(self._root.name, f"http_{outcome.http_status}")
            return
        if outcome.move is MoveAction.PROCESSED:
            await self._move(relpath, MoveAction.PROCESSED, outcome.status or "ingested")
        elif outcome.move is MoveAction.FAILED:
            await self._move(relpath, MoveAction.FAILED, outcome.detail or outcome.status or "failed")
        else:  # LEAVE → bounded retry
            await self._schedule_retry(relpath, attempt, "retry")

    async def _move(self, relpath: str, action: MoveAction, reason: str) -> None:
        subdir = (
            self._root.processed_subdir
            if action is MoveAction.PROCESSED
            else self._root.failed_subdir
        )
        try:
            new_rel = await self._provider.move_to_subdir(relpath, subdir)
            logger.info("root %s: %s → %s (%s)", self._root.name, relpath, new_rel, reason)
        finally:
            self._inflight.discard(relpath)
        if action is MoveAction.FAILED and self._on_failed is not None:
            await self._on_failed(self._root.name, relpath, reason)

    async def _schedule_retry(self, relpath: str, attempt: int, reason: str) -> None:
        if attempt + 1 >= self._max_retries:
            # Give up: leave the file in the inbox + release it (a future event or
            # the next restart's reconciliation will re-try). Notify the operator.
            self._inflight.discard(relpath)
            logger.warning(
                "root %s: %s exhausted %d retries (%s); leaving in inbox",
                self._root.name, relpath, self._max_retries, reason,
            )
            if self._on_failed is not None:
                await self._on_failed(self._root.name, relpath, f"retry_exhausted: {reason}")
            return

        delay = self._retry_base * (2 ** attempt)

        async def _later() -> None:
            try:
                await asyncio.sleep(delay)
                if self._stopped.is_set():
                    self._inflight.discard(relpath)
                    return
                await self._process(relpath, attempt + 1)  # stays inflight across the wait
            finally:
                self._retry_tasks.discard(asyncio.current_task())

        task = asyncio.create_task(_later())
        self._retry_tasks.add(task)
