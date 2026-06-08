"""renfield-mcp-filesystem — watch-folder MCP server.

Event-driven detection of settled new files on local/SMB shares (NEVER polling),
pushed into Renfield over REST (`POST /api/folder-ingest/document`). The dedicated
server is the sole access boundary to the shares — the backend never mounts them.
"""

__version__ = "0.1.4"