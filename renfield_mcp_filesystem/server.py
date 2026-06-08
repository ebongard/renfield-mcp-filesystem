#!/usr/bin/env python3
"""renfield-mcp-filesystem — the FastMCP server.

Runs the streamable-http MCP server (interactive tools) AND launches one
event-driven watcher daemon per configured root (via the lifespan). The watch
loop pushes settled files into Renfield; the tools let the agent browse + ingest
on demand. Core logic lives in mcp-free modules (config/registry/tools/engine);
this file is the thin MCP + asyncio-orchestration shell.

Env: RENFIELD_URL, RENFIELD_INGEST_TOKEN, FILES_ROOTS_YAML, FILES_* (see config),
FILES_MCP_HOST (default 0.0.0.0), FILES_MCP_PORT (default 8080).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager

from mcp.server.fastmcp import FastMCP

from . import tools as t
from .config import load_config
from .daemon import DaemonManager

logging.basicConfig(
    level=os.environ.get("FILES_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,  # MCP stdio safety + container logs
)
logger = logging.getLogger("renfield-mcp-filesystem")

_manager: DaemonManager | None = None


@asynccontextmanager
async def _lifespan(_server: FastMCP):
    """Start the watcher daemons (+ dynamic-roots reload) on startup; stop them
    on shutdown."""
    global _manager
    config = load_config()
    _manager = DaemonManager(config)
    await _manager.start()
    try:
        yield {}
    finally:
        await _manager.stop()


mcp = FastMCP(
    "renfield-mcp-filesystem",
    lifespan=_lifespan,
    host=os.environ.get("FILES_MCP_HOST", "0.0.0.0"),
    port=int(os.environ.get("FILES_MCP_PORT", "8080")),
)


def _reg() -> DaemonManager:
    if _manager is None:
        raise RuntimeError("daemon manager not initialised")
    return _manager


@mcp.tool()
async def list_watch_folders() -> dict:
    """List the configured watch roots and whether each is currently connected."""
    return await t.list_watch_folders(_reg())


@mcp.tool()
async def list_files(root: str, pattern: str | None = None) -> dict:
    """List files in a watch root's inbox (excludes the processed/failed subdirs).
    Returns each file's qualified ``path`` ('<root>/<relpath>') for the other tools."""
    return await t.list_files(_reg(), root, pattern)


@mcp.tool()
async def get_file_info(path: str) -> dict:
    """Stat a file by its qualified ``path`` ('<root>/<relpath>')."""
    return await t.get_file_info(_reg(), path)


@mcp.tool()
async def read_file(path: str, truncate: bool = True) -> dict:
    """Read a file as base64 by qualified ``path``. Pass ``truncate=False`` for
    the full bytes (the Renfield ingest path); the default caps the payload."""
    return await t.read_file(_reg(), path, truncate)


@mcp.tool()
async def move_file(path: str, subdir: str) -> dict:
    """Move a file (by qualified ``path``) into ``subdir`` within its root."""
    return await t.move_file(_reg(), path, subdir)


def main() -> None:
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
