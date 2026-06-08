"""Provider registry — builds one provider + one ingest engine per configured
root, and holds the live providers so the interactive MCP tools and the watcher
daemons share the same connected clients.
"""

from __future__ import annotations

import logging

from .config import Config, LocalRoot, Root, SmbRoot
from .engine import IngestEngine
from .providers.base import FolderProvider
from .providers.local import LocalProvider
from .pusher import RenfieldPusher

logger = logging.getLogger("renfield-mcp-filesystem.registry")


def make_provider(root: Root, settle_seconds: float = 2.0) -> FolderProvider:
    if isinstance(root, LocalRoot):
        return LocalProvider(root)
    if isinstance(root, SmbRoot):
        from .providers.smb import SmbProvider

        return SmbProvider(root, settle_seconds=settle_seconds)
    raise ValueError(f"unknown root type for {root.name!r}")


class ProviderRegistry:
    def __init__(self, config: Config):
        self._config = config
        self._pusher = RenfieldPusher(
            config.renfield_url, config.ingest_token, config.push_timeout_seconds
        )
        self._providers: dict[str, FolderProvider] = {}
        self._engines: dict[str, IngestEngine] = {}
        self.build()

    def build(self) -> None:
        for root in self._config.roots:
            provider = make_provider(root, settle_seconds=self._config.settle_seconds)
            self._providers[root.name] = provider
            self._engines[root.name] = IngestEngine(
                config=self._config,
                root=root,
                provider=provider,
                pusher=self._pusher,
                on_failed=self._on_failed,
                on_fatal=self._on_fatal,
            )

    async def _on_failed(self, root: str, relpath: str, reason: str) -> None:
        # Operator-notification + per-root health land in T17; loud log for now.
        logger.warning("FAILURE root=%s file=%s reason=%s", root, relpath, reason)

    async def _on_fatal(self, root: str, reason: str) -> None:
        logger.error("FATAL root=%s reason=%s — root stopped", root, reason)

    # -- accessors for the tools --

    def names(self) -> list[str]:
        return list(self._providers)

    def get(self, name: str) -> FolderProvider | None:
        return self._providers.get(name)

    def engines(self) -> list[IngestEngine]:
        return list(self._engines.values())
