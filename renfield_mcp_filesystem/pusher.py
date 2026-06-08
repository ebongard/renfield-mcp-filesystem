"""The REST push client — POSTs a settled file to Renfield's folder-ingest
endpoint and resolves the response into a move decision.

Transport mapping (the cross-repo contract):
  - 200 body ``{status, document_id, detail, contract_version}`` → move by status
    (ingested|duplicate → processed/, failed → failed/, retry → leave).
  - 401/403 → FATAL config error (wrong/missing token): never move the file;
    the daemon surfaces it loudly and stops touching this root.
  - 503 / network error / any other code → treat as retry (leave in inbox) —
    a file is never moved/lost on a transient or unknown outcome (DX-7).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import httpx

from .contract import (
    CONTRACT_HEADER,
    FOLDER_INGEST_CONTRACT_VERSION,
    INGEST_PATH,
    MoveAction,
    move_action_for,
)

logger = logging.getLogger("renfield-mcp-filesystem.pusher")


@dataclass
class PushOutcome:
    move: MoveAction
    fatal: bool = False  # 401/403 — operator must fix the token; stop the root
    status: str | None = None
    document_id: int | None = None
    detail: str | None = None
    http_status: int | None = None


class RenfieldPusher:
    def __init__(self, renfield_url: str, ingest_token: str, timeout_seconds: float = 120.0):
        self._url = renfield_url.rstrip("/") + INGEST_PATH
        self._headers = {
            "Authorization": f"Bearer {ingest_token}",
            CONTRACT_HEADER: FOLDER_INGEST_CONTRACT_VERSION,
        }
        self._timeout = timeout_seconds

    async def push(
        self,
        *,
        file_bytes: bytes,
        filename: str,
        root: str,
        relpath: str,
        sha256: str,
        mime: str | None,
    ) -> PushOutcome:
        metadata = json.dumps(
            {
                "filename": filename,
                "root": root,
                "relpath": relpath,
                "sha256": sha256,
                "mime": mime,
            }
        )
        files = {"file": (filename, file_bytes, mime or "application/octet-stream")}
        data = {"metadata": metadata}
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(
                    self._url, headers=self._headers, files=files, data=data
                )
        except httpx.HTTPError as exc:
            logger.warning("push for %s/%s failed at transport: %s", root, relpath, exc)
            return PushOutcome(move=MoveAction.LEAVE, detail=f"transport_error: {exc}")

        if resp.status_code in (401, 403):
            logger.error(
                "push for %s/%s rejected with HTTP %s — token/config error (fatal)",
                root, relpath, resp.status_code,
            )
            return PushOutcome(move=MoveAction.LEAVE, fatal=True, http_status=resp.status_code)

        if resp.status_code != 200:
            # 503 (disabled / worker down) and anything else → retry, never lose.
            logger.info(
                "push for %s/%s got HTTP %s — leaving in inbox to retry",
                root, relpath, resp.status_code,
            )
            return PushOutcome(move=MoveAction.LEAVE, http_status=resp.status_code)

        try:
            body = resp.json()
        except (json.JSONDecodeError, ValueError):
            logger.warning("push for %s/%s: 200 but unparseable body; will retry", root, relpath)
            return PushOutcome(move=MoveAction.LEAVE, http_status=200)

        status = body.get("status")
        move = move_action_for(str(status))
        skew = body.get("contract_version")
        if skew and skew != FOLDER_INGEST_CONTRACT_VERSION:
            logger.warning(
                "contract skew: backend %s, MCP %s (processing leniently)",
                skew, FOLDER_INGEST_CONTRACT_VERSION,
            )
        return PushOutcome(
            move=move,
            status=str(status) if status is not None else None,
            document_id=body.get("document_id"),
            detail=body.get("detail"),
            http_status=200,
        )
