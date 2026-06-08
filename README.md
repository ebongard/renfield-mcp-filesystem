# renfield-mcp-filesystem

Watch-folder MCP server for [Renfield](https://github.com/ebongard/renfield): it
watches folders (local / SMB) for **settled new files** and **pushes** them into
Renfield over REST (`POST /api/folder-ingest/document`), which ingests them into
the knowledge base and Paperless.

This dedicated server is the **sole access boundary** to the shares — the
Renfield backend never mounts them, and credentials/clients live only here. A new
off-cluster share or a per-user folder is added at **runtime** by editing the
roots config — no redeploy, no static volume.

## Principles

- **Event-driven, never polling.** Local roots use `watchdog` (inotify
  `CLOSE_WRITE` = the settle signal); SMB roots use SMB2 `CHANGE_NOTIFY` + an
  event-debounce timer. No periodic filesystem scan. (One exception: a single
  enumeration at startup catches files that already existed before the watch
  began — a one-shot catch-up, not a poll.)
- **Create-only.** Acts on settled new files; ignores in-place rewrites of files
  it has already handled.
- **The backend's 4-state response drives the move.** `ingested|duplicate` →
  `processed/`, `failed` → `failed/`, `retry` → left in place and re-attempted on
  a bounded backoff. `401/403` is a fatal token error (the file is never moved).
- **Local safety gates.** Size ceiling + extension allowlist are enforced
  *before* any push (an oversized / disallowed file goes straight to `failed/`).

## Quickstart (local folder)

Point it at a local inbox and an existing Renfield backend:

```bash
mkdir -p ./inbox
cat > roots.yaml <<'YAML'
roots:
  - name: inbox
    type: local
    path: /watch/inbox
YAML

docker run --rm \
  -e RENFIELD_URL=http://renfield-backend:8000 \
  -e RENFIELD_INGEST_TOKEN=<token-from-POST-/api/folder-ingest/token> \
  -e FILES_ROOTS_YAML=/config/roots.yaml \
  -v "$PWD/roots.yaml:/config/roots.yaml:ro" \
  -v "$PWD/inbox:/watch/inbox" \
  -p 8080:8080 \
  registry.treehouse.x-idra.de/renfield/filesystem-mcp:latest
```

Drop a PDF into `./inbox` → it appears in Renfield's `/wissen` and Paperless, then
moves to `./inbox/processed/`. A rejected file moves to `./inbox/failed/`.

**Mint the token** on the backend (admin): `POST /api/folder-ingest/token`. The
backend feature must be on (`FOLDER_INGEST_ENABLED=true`).

## Configuration

Global settings come from the environment; the watch **roots** come from a
mounted `roots.yaml` (see `config/roots.example.yaml`).

| Env var | Default | Meaning |
|---|---|---|
| `RENFIELD_URL` | — (required) | Renfield backend base URL |
| `RENFIELD_INGEST_TOKEN` | — (required) | folder-ingest Bearer token |
| `FILES_ROOTS_YAML` | — | path to the mounted roots.yaml (reloaded on change) |
| `FILES_ALLOWED_EXTENSIONS` | `pdf,docx,...` | local extension allowlist |
| `FILES_MAX_FILE_SIZE_MB` | `50` | size ceiling (enforced before push) |
| `FILES_SETTLE_SECONDS` | `2.0` | SMB settle-debounce window |
| `FILES_MCP_HOST` / `FILES_MCP_PORT` | `0.0.0.0` / `8080` | MCP server bind |
| `FILES_NOTIFY_WEBHOOK_URL` / `_TOKEN` | — | optional failure/disconnect webhook |

`roots.yaml` (creds referenced by env-var name, never inlined):

```yaml
roots:
  - name: documents
    type: smb
    server: nas.example.lan
    share: Documents
    path: Inbox
    username_env: DOCS_SMB_USER
    password_env: DOCS_SMB_PASS
  - name: local-inbox
    type: local
    path: /watch/inbox
```

Each root takes an optional `processed_subdir` / `failed_subdir` (defaults
`processed` / `failed`). After a file is handled it is moved out of the inbox:
ingested/duplicate → `processed`, rejected → `failed`, retry → left in place.

**Where the processed/failed dirs live differs by provider:**

- **SMB** — at the **share root**, as *siblings* of the watched `path`. A root
  with `path: Inbox` produces `<share>/{Inbox, processed, failed}` (not
  `<share>/Inbox/processed`). With `path: ""` (watch the share root) they are
  simply the two top-level dirs. The watched inbox + both dirs are auto-created
  on connect.
- **local** — *nested* inside the watched `path` (`<path>/processed`,
  `<path>/failed`), since a local root is self-contained.

## Dry-run (preflight)

Validate config + credentials + the matched/skipped files before the daemon
touches anything (pushes nothing, moves nothing):

```bash
renfield-mcp-filesystem-scan --dry-run
# root documents (smb):
#   would push (2): invoice.pdf (12345 bytes), letter.pdf (6789 bytes)
#   skipped (1): notes.exe (extension_not_allowed)
```

## Interactive MCP tools

Registered as `mcp.files.*` (the `files` stanza in Renfield's
`config/mcp_servers.yaml`). The agent uses these to browse + ingest on demand
(the watch loop is automatic + event-driven):

- `list_watch_folders()` → roots + `connected` + `last_error`
- `list_files(root, pattern?)` → files, each with a qualified `path` `"<root>/<relpath>"`
- `get_file_info(path)` · `read_file(path, truncate?)` · `move_file(path, subdir)`

The Renfield agent tool `internal.ingest_file({path})` pulls bytes via
`read_file(path, truncate=False)` and runs them through the same ingest bridge.

## Deploy (k8s)

Manifests in `k8s/` (ConfigMap roots + Secret creds + a single-replica
Deployment + Service). Single replica by design — two would double-push. Build
on the build box → Harbor → `kubectl apply -f k8s/`.

## Develop

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest
```

Core modules (config/contract/providers/pusher/engine/gate/daemon/tools/scan)
are mcp-free and fully unit-tested; the live inotify/SMB CHANGE_NOTIFY wiring and
the cross-repo push are verified by the Renfield `.159` E2E. NFS is deferred — it
has no native change-notification, so it cannot be event-driven without polling.
