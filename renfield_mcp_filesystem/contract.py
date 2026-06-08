"""The cross-repo folder-ingest contract — the wire shape this server and the
Renfield backend (`api/routes/folder_ingest.py`) BOTH depend on.

Keep ``FOLDER_INGEST_CONTRACT_VERSION`` + the status names in lock-step with the
backend's ``services/folder_ingest.py``. Bump the version on ANY change here, and
update the backend's matching constant. The backend pins these in
``tests/backend/test_folder_ingest_route.py`` (the contract lock test).
"""

from __future__ import annotations

from enum import Enum

# Mirror of the backend's FOLDER_INGEST_CONTRACT_VERSION. Sent on every push in
# the request header below; the backend echoes its own version in the response.
FOLDER_INGEST_CONTRACT_VERSION = "1"
CONTRACT_HEADER = "X-Folder-Ingest-Contract"

# The push endpoint path on the Renfield backend.
INGEST_PATH = "/api/folder-ingest/document"
HEALTH_PATH = "/api/folder-ingest/health"


class IngestStatus(str, Enum):
    """The 4-state response the backend returns; this server moves the source
    file by it. Names are part of the cross-repo seam — do not rename without a
    contract-version bump."""

    INGESTED = "ingested"
    DUPLICATE = "duplicate"
    RETRY = "retry"
    FAILED = "failed"


class MoveAction(str, Enum):
    """What the watcher does with the source file after a push."""

    PROCESSED = "processed"  # move → processed_subdir
    FAILED = "failed"  # move → failed_subdir
    LEAVE = "leave"  # leave in place; it will be re-pushed later


# The status → move decision. An UNKNOWN status (contract skew, DX-7) maps to
# LEAVE so a file is never moved/lost on a version the MCP doesn't understand.
_MOVE_BY_STATUS = {
    IngestStatus.INGESTED: MoveAction.PROCESSED,
    IngestStatus.DUPLICATE: MoveAction.PROCESSED,
    IngestStatus.FAILED: MoveAction.FAILED,
    IngestStatus.RETRY: MoveAction.LEAVE,
}


def move_action_for(status: str) -> MoveAction:
    """Map a backend status string to a move action. Unknown / unparseable →
    LEAVE (re-push later) so contract skew never moves a file to the wrong place."""
    try:
        return _MOVE_BY_STATUS[IngestStatus(status)]
    except ValueError:
        return MoveAction.LEAVE