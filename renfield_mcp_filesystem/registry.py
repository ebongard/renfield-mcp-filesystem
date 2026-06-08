"""Provider factory — maps a Root config to its FolderProvider impl."""

from __future__ import annotations

from .config import LocalRoot, Root, SmbRoot
from .providers.base import FolderProvider
from .providers.local import LocalProvider


def make_provider(root: Root, settle_seconds: float = 2.0) -> FolderProvider:
    if isinstance(root, LocalRoot):
        return LocalProvider(root)
    if isinstance(root, SmbRoot):
        from .providers.smb import SmbProvider

        return SmbProvider(root, settle_seconds=settle_seconds)
    raise ValueError(f"unknown root type for {root.name!r}")
