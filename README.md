# renfield-mcp-filesystem

Watch-folder MCP server for [Renfield](https://github.com/ebongard/renfield): it
watches folders (local / SMB) for **settled new files** and **pushes** them into
Renfield over REST (`POST /api/folder-ingest/document`), which ingests them into
the knowledge base and Paperless.

This dedicated server is the **sole access boundary** to the shares — the
Renfield backend never mounts them, and credentials/clients live only here.

## Principles

- **Event-driven, never polling.** Local roots use `watchdog` (inotify
  `CLOSE_WRITE` = the settle signal); SMB roots use SMB2 `CHANGE_NOTIFY` + an
  event-debounce timer. There is no periodic filesystem scan. (One exception: a
  **one-shot** enumeration at startup catches files that already existed before
  the watch began — a single catch-up, not a poll.)
- **Create-only.** Acts on settled new files; ignores in-place rewrites of files
  it has already handled.
- **The backend's 4-state response drives the move.** `ingested|duplicate` →
  `processed/`, `failed` → `failed/`, `retry` → left in place and re-attempted on
  a bounded backoff. `401/403` is a fatal token error (the file is never moved).
- **Local safety gates.** Size ceiling + extension allowlist are enforced *before*
  any push (an oversized / disallowed file goes straight to `failed/`).

## Status

Early build. Done: config + roots model, the contract, the event-driven local
provider, the REST pusher, and the per-root ingest engine (with one-shot startup
reconciliation + bounded retry). **In progress:** the SMB provider, the FastMCP
server + interactive tools (`list_watch_folders` / `list_files` / `read_file` /
`get_file_info` / `move_file`), dynamic root reload, dry-run, disconnect handling,
and the Docker/k8s distribution.

## Develop

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest
```
