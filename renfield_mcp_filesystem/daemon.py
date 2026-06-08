"""DaemonManager — owns the live providers + per-root watcher daemons, and
**reloads roots at runtime** when the mounted ``roots.yaml`` changes (T13). This
is the reason the dedicated MCP exists: a new off-cluster share or a per-user
folder is added by editing the (ConfigMap-mounted) YAML — no redeploy, no pod
recreate, no static volume.

Providers are built eagerly in ``__init__`` (so the interactive tools' ``get`` /
``names`` work before the daemon starts); ``start()`` launches one watcher task
per root + the event-driven YAML watch. A roots.yaml change diffs the new roots
against the running set and starts/stops/rebuilds only what changed. A malformed
YAML is logged and ignored (the running roots keep going).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .config import Config, Root, load_roots
from .engine import FailureHook, FatalHook, IngestEngine
from .providers.base import FolderProvider
from .pusher import RenfieldPusher
from .registry import make_provider

logger = logging.getLogger("renfield-mcp-filesystem.daemon")

# Coalesce the multiple writes an editor / ConfigMap swap produces before
# reloading. A debounce timer, not a poll.
_RELOAD_DEBOUNCE_S = 1.0


class DaemonManager:
    def __init__(
        self,
        config: Config,
        *,
        provider_factory=make_provider,
        on_failed: FailureHook | None = None,
        on_fatal: FatalHook | None = None,
    ):
        self._config = config
        self._make_provider = provider_factory
        self._on_failed = on_failed
        self._on_fatal = on_fatal
        self._pusher = RenfieldPusher(
            config.renfield_url, config.ingest_token, config.push_timeout_seconds
        )
        self._roots: dict[str, Root] = {}
        self._providers: dict[str, FolderProvider] = {}
        self._engines: dict[str, IngestEngine] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._yaml_observer: Observer | None = None
        self._reload_handle: asyncio.TimerHandle | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        for root in config.roots:
            self._build_root(root)

    # -- lifecycle --

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        for name in list(self._engines):
            self._launch(name)
        if self._config.roots_path:
            self._watch_yaml(self._config.roots_path)
        logger.info("daemon started with roots: %s", self.names())

    async def stop(self) -> None:
        if self._yaml_observer is not None:
            self._yaml_observer.stop()
            await asyncio.to_thread(self._yaml_observer.join, 5)
            self._yaml_observer = None
        if self._reload_handle is not None:
            self._reload_handle.cancel()
        for name in list(self._roots):
            await self._stop_root(name)

    async def reload(self) -> None:
        """Re-read the roots YAML and apply the diff. Bad YAML → keep current."""
        if not self._config.roots_path:
            return
        try:
            new_roots = load_roots(self._config.roots_path)
        except Exception as exc:  # noqa: BLE001 - never crash on a bad edit
            logger.error("roots reload failed (keeping current roots): %s", exc)
            return
        await self._apply_roots(new_roots)
        logger.info("roots reloaded: %s", self.names())

    # -- build / launch / stop --

    def _build_root(self, root: Root) -> None:
        provider = self._make_provider(root, self._config.settle_seconds)
        engine = IngestEngine(
            config=self._config, root=root, provider=provider, pusher=self._pusher,
            on_failed=self._on_failed, on_fatal=self._on_fatal,
        )
        self._roots[root.name] = root
        self._providers[root.name] = provider
        self._engines[root.name] = engine

    def _launch(self, name: str) -> None:
        self._tasks[name] = asyncio.create_task(self._engines[name].run())

    async def _stop_root(self, name: str) -> None:
        task = self._tasks.pop(name, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        engine = self._engines.pop(name, None)
        if engine is not None:
            try:
                await engine.stop()
            except Exception as exc:  # noqa: BLE001
                logger.warning("error stopping engine %s: %s", name, exc)
        self._providers.pop(name, None)
        self._roots.pop(name, None)

    async def _apply_roots(self, new_roots: list[Root]) -> None:
        new_by_name = {r.name: r for r in new_roots}
        for name in set(self._roots) - set(new_by_name):
            logger.info("root %s removed — stopping", name)
            await self._stop_root(name)
        for name, root in new_by_name.items():
            current = self._roots.get(name)
            if current is None:
                logger.info("root %s added — starting", name)
                self._build_root(root)
                self._launch(name)
            elif current != root:
                logger.info("root %s changed — restarting", name)
                await self._stop_root(name)
                self._build_root(root)
                self._launch(name)

    # -- yaml watch (event-driven reload trigger) --

    def _watch_yaml(self, roots_path: str) -> None:
        yaml_path = Path(roots_path).resolve()
        handler = _YamlChangeHandler(yaml_path, self._schedule_reload_threadsafe)
        self._yaml_observer = Observer()
        self._yaml_observer.schedule(handler, str(yaml_path.parent), recursive=False)
        self._yaml_observer.start()

    def _schedule_reload_threadsafe(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._debounce_reload)

    def _debounce_reload(self) -> None:
        if self._reload_handle is not None:
            self._reload_handle.cancel()
        assert self._loop is not None
        self._reload_handle = self._loop.call_later(
            _RELOAD_DEBOUNCE_S, lambda: asyncio.create_task(self.reload())
        )

    # -- accessors for the tools --

    def names(self) -> list[str]:
        return list(self._providers)

    def get(self, name: str) -> FolderProvider | None:
        return self._providers.get(name)


class _YamlChangeHandler(FileSystemEventHandler):
    """Fires ``on_change`` when the watched roots.yaml is touched. Watches the
    parent dir because a ConfigMap update swaps the file via a ``..data``
    symlink (the file's own inode changes)."""

    def __init__(self, yaml_path: Path, on_change):
        self._name = yaml_path.name
        self._on_change = on_change

    def _relevant(self, path: str | None) -> bool:
        if not path:
            return False
        name = Path(path).name
        return name == self._name or name == "..data"

    def on_modified(self, event):
        if self._relevant(getattr(event, "src_path", None)):
            self._on_change()

    def on_created(self, event):
        if self._relevant(getattr(event, "src_path", None)):
            self._on_change()

    def on_moved(self, event):
        if self._relevant(getattr(event, "dest_path", None)) or self._relevant(
            getattr(event, "src_path", None)
        ):
            self._on_change()
