"""Interactive MCP tool implementations (mcp-free, so they're unit-testable).

The agent path: ``list_watch_folders`` → ``list_files(root)`` → act on a file by
its qualified ``path`` = ``"<root>/<relpath>"``. ``read_file`` returns base64;
the Renfield backend's ``internal.ingest_file`` calls it with ``truncate=False``
to get the full bytes (the MCP-manager passes ``truncate=False`` to bypass its
128 KB response cap). These never drive the watch loop — that's event-driven.
"""

from __future__ import annotations

import base64
import os

from .daemon import DaemonManager
from .providers.base import safe_relpath

# When truncate=True (interactive/LLM use) cap the returned content so the
# base64 stays under the MCP-manager's 128 KB response cap. truncate=False
# (the backend's ingest path) returns the full file.
_TRUNCATE_BYTES = 90 * 1024


def _err(msg: str) -> dict:
    return {"error": msg}


def split_path(path: str) -> tuple[str, str]:
    """Split a qualified ``"<root>/<relpath>"`` into (root, relpath).

    The relpath is validated against traversal (review H5) — ``..``/absolute/
    drive/UNC/ADS are rejected here so no tool entrypoint can escape the root,
    independent of the provider's own guard.
    """
    root, sep, relpath = (path or "").partition("/")
    if not sep or not root or not relpath:
        raise ValueError("path must be '<root>/<relpath>'")
    relpath = safe_relpath(relpath)
    return root, relpath


async def list_watch_folders(reg: DaemonManager) -> dict:
    roots = []
    for name in reg.names():
        p = reg.get(name)
        roots.append(
            {
                "name": name,
                "connected": p.connected if p else False,
                "last_error": p.last_error if p else "unknown root",
            }
        )
    return {"roots": roots}


async def list_files(reg: DaemonManager, root: str, pattern: str | None = None) -> dict:
    provider = reg.get(root)
    if provider is None:
        return _err(f"unknown root: {root!r}")
    files = await provider.list_files(pattern)
    return {
        "root": root,
        "files": [
            {"path": f"{root}/{fi.relpath}", "relpath": fi.relpath, "size": fi.size}
            for fi in files
        ],
    }


async def get_file_info(reg: DaemonManager, path: str) -> dict:
    try:
        root, relpath = split_path(path)
    except ValueError as exc:
        return _err(str(exc))
    provider = reg.get(root)
    if provider is None:
        return _err(f"unknown root: {root!r}")
    try:
        info = await provider.stat(relpath)
    except ValueError as exc:  # traversal rejected by the provider
        return _err(str(exc))
    if info is None:
        return _err(f"not found: {path!r}")
    return {"path": path, "relpath": relpath, "size": info.size, "mtime": info.mtime}


async def read_file(reg: DaemonManager, path: str, truncate: bool = True) -> dict:
    try:
        root, relpath = split_path(path)
    except ValueError as exc:
        return _err(str(exc))
    provider = reg.get(root)
    if provider is None:
        return _err(f"unknown root: {root!r}")
    try:
        info = await provider.stat(relpath)
        if info is None:
            return _err(f"not found: {path!r}")
        data = await provider.read_bytes(relpath)
    except ValueError as exc:  # traversal rejected by the provider
        return _err(str(exc))
    truncated = False
    if truncate and len(data) > _TRUNCATE_BYTES:
        data = data[:_TRUNCATE_BYTES]
        truncated = True
    return {
        "path": path,
        "filename": os.path.basename(relpath),
        "size": info.size,
        "truncated": truncated,
        "content_base64": base64.b64encode(data).decode("ascii"),
    }


async def move_file(reg: DaemonManager, path: str, subdir: str) -> dict:
    try:
        root, relpath = split_path(path)
    except ValueError as exc:
        return _err(str(exc))
    provider = reg.get(root)
    if provider is None:
        return _err(f"unknown root: {root!r}")
    # subdir must be a single, traversal-free path component. Reject BOTH
    # separators (a `\`-separated `..\..\evil` previously slipped past a `/`-only
    # check and escaped via _root_unc) plus drive/ADS colons (review H5 follow-up).
    if (
        not subdir
        or "/" in subdir
        or "\\" in subdir
        or ":" in subdir
        or subdir in (".", "..")
    ):
        return _err(f"invalid subdir: {subdir!r}")
    try:
        new_rel = await provider.move_to_subdir(relpath, subdir)
    except ValueError as exc:  # traversal rejected by the provider
        return _err(str(exc))
    return {"moved_to": f"{root}/{new_rel}"}
