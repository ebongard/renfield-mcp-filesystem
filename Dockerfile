# renfield-mcp-filesystem — watch-folder MCP server.
# Pure-Python deps (watchdog uses inotify on Linux; smbprotocol is pure Python),
# so no apt build layer is needed. NFS is deferred, so no libnfs.
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY renfield_mcp_filesystem ./renfield_mcp_filesystem
RUN pip install --no-cache-dir .

# Streamable-http MCP server (matches the `files` stanza in mcp_servers.yaml).
ENV FILES_MCP_HOST=0.0.0.0 \
    FILES_MCP_PORT=8080
EXPOSE 8080

# Required at runtime: RENFIELD_URL, RENFIELD_INGEST_TOKEN, FILES_ROOTS_YAML
# (+ the SMB credential env vars referenced by roots.yaml). See README.
ENTRYPOINT ["renfield-mcp-filesystem"]
