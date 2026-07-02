"""Per-root ingest engine: consumes the provider's event-driven ``watch()`` and
drives each settled file through gate → hash → push → move.

Detection is event-driven (NEVER a poll loop). Two non-poll complements:
  1. **One-shot startup reconciliation** — inotify only reports events that occur
     AFTER the watch starts, so files already in the inbox at boot would be
     invisible forever. We enumerate the inbox ONCE at startup and dispatch those
     files, then rely purely on events. This is a single catch-up, not a periodic
     scan.
  2. **Bounded per-file retry** — a ``retry`` response leaves the file in place
     and re-attempts THAT file on a capped exponential backoff (a retry schedule
     for one known unit of work, not a filesystem poll). Budgets differ by reason
     (see ``_schedule_retry``): a backend ``retry`` (the document is still being
     OCR'd — an inbox burst can keep re-pushes returning RETRY for many minutes)
     gets a LARGE budget so the backlog doesn't strand it; a local processing
     error is capped tight; a 0-byte ``empty`` read (mid-copy on CLOSE_WRITE-less
     SMB) gets a short wait then fails only if it never gains bytes. After the
     backend/error cap we give up and leave it (the next reconcile re-tries).

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
        backend_retry_max: int = 480,
        empty_retry_max: int = 8,
        retry_max_delay: float = 60.0,
        push_semaphore: asyncio.Semaphore | None = None,
    ):
        self._config = config
        self._root = root
        self._provider = provider
        self._pusher = pusher
        self._on_failed = on_failed
        self._on_fatal = on_fatal
        # Differentiated retry budgets by reason (see _schedule_retry):
        #   local processing_error → small cap (a bug shouldn't retry forever)
        #   backend "retry"        → large cap (survive a minutes/hours OCR backlog)
        #   "empty" (0-byte)       → medium cap, then FAIL (genuinely empty)
        self._max_retries = max_retries
        self._retry_base = retry_base_seconds
        self._backend_retry_max = backend_retry_max
        self._empty_retry_max = empty_retry_max
        self._retry_max_delay = retry_max_delay
        # Shared across all roots + retries (built by the daemon) so a burst can
        # never fan out into a flood of simultaneous pushes. A no-op unbounded
        # fallback keeps the engine usable standalone (tests).
        self._push_sem = push_semaphore or asyncio.Semaphore(2**31)
        self._inflight: set[str] = set()
        # relpath -> (last retry reason, CONSECUTIVE count of that reason). The
        # budget is per consecutive-reason run, not a single cumulative counter,
        # so a lone spurious 0-byte read mid-backend-retry can't inherit the high
        # backend count and trip the tiny empty cap (see _schedule_retry).
        self._retry_state: dict[str, tuple[str, int]] = {}
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
        await self._process(relpath)

    async def _process(self, relpath: str) -> None:
        try:
            info = await self._provider.stat(relpath)
            if info is None:
                self._release(relpath)  # already moved / vanished
                return
            ok, reason = classify(self._config, relpath, info.size)
            if not ok:
                if reason == "empty":
                    # 0 bytes at settle is almost always a mid-copy read (SMB has
                    # no CLOSE_WRITE, so the settle debouncer can fire between
                    # write chunks). Retry to let the copy finish instead of
                    # terminal-rejecting; a file that stays empty across the
                    # budget fails in _schedule_retry.
                    await self._schedule_retry(relpath, "empty")
                    return
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
            await self._apply_outcome(relpath, outcome)
        except Exception as exc:  # noqa: BLE001 - never drop a file on a bug
            logger.exception("root %s: error processing %s: %s", self._root.name, relpath, exc)
            await self._schedule_retry(relpath, "processing_error")

    async def _apply_outcome(self, relpath: str, outcome: PushOutcome) -> None:
        if outcome.fatal:
            self._release(relpath)
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
            await self._schedule_retry(relpath, "retry")

    def _release(self, relpath: str) -> None:
        """Drop all per-file processing state (a file has reached a terminal
        outcome or vanished). Keeps _inflight and _retry_state from leaking."""
        self._inflight.discard(relpath)
        self._retry_state.pop(relpath, None)

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
            self._release(relpath)
        if action is MoveAction.FAILED and self._on_failed is not None:
            await self._on_failed(self._root.name, relpath, reason)

    async def _schedule_retry(self, relpath: str, reason: str) -> None:
        # Budget by reason, counted per CONSECUTIVE run of that reason (NOT a
        # single cumulative counter). _process re-stats on every attempt, so a
        # lone spurious 0-byte read during a long backend-retry run must not
        # inherit that run's high count and instantly trip the tiny empty cap —
        # which would move a real, already-pushed document to failed/. A reason
        # change resets the count. Budgets: backend "retry" (doc still OCR'ing)
        # survives a long backlog; local processing_error is capped tight; an
        # "empty" 0-byte read gets a short wait then FAILS if it never gains bytes.
        cap = {
            "retry": self._backend_retry_max,
            "empty": self._empty_retry_max,
        }.get(reason, self._max_retries)

        prev_reason, count = self._retry_state.get(relpath, (None, 0))
        count = count + 1 if prev_reason == reason else 1

        if count >= cap:
            self._retry_state.pop(relpath, None)
            if reason == "empty":
                # Stayed 0 bytes across the whole wait → genuinely empty. Terminal.
                logger.warning(
                    "root %s: %s still empty after %d checks; moving to failed/",
                    self._root.name, relpath, cap,
                )
                await self._move(relpath, MoveAction.FAILED, "empty")
                return
            # Give up: leave the file in the inbox + release it (a future event or
            # the next reconcile will re-try). Notify the operator.
            self._inflight.discard(relpath)
            logger.warning(
                "root %s: %s exhausted %d retries (%s); leaving in inbox",
                self._root.name, relpath, cap, reason,
            )
            if self._on_failed is not None:
                await self._on_failed(self._root.name, relpath, f"retry_exhausted: {reason}")
            return

        self._retry_state[relpath] = (reason, count)
        # Exponential backoff on the consecutive count, capped so the interval
        # plateaus (a 480-attempt backend budget must not compute 2**479 or sleep
        # for years). Clamp the exponent before the (big-int) shift, then the delay.
        delay = min(self._retry_max_delay, self._retry_base * (2 ** min(count - 1, 16)))

        async def _later() -> None:
            try:
                await asyncio.sleep(delay)
                if self._stopped.is_set():
                    self._release(relpath)
                    return
                await self._process(relpath)  # stays inflight across the wait
            finally:
                self._retry_tasks.discard(asyncio.current_task())

        task = asyncio.create_task(_later())
        self._retry_tasks.add(task)
