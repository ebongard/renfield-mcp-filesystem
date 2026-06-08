from renfield_mcp_filesystem.contract import (
    FOLDER_INGEST_CONTRACT_VERSION,
    IngestStatus,
    MoveAction,
    move_action_for,
)


def test_contract_version_pinned():
    # Must match the Renfield backend's FOLDER_INGEST_CONTRACT_VERSION.
    assert FOLDER_INGEST_CONTRACT_VERSION == "1"


def test_status_values():
    assert {s.value for s in IngestStatus} == {"ingested", "duplicate", "retry", "failed"}


def test_move_action_mapping():
    assert move_action_for("ingested") is MoveAction.PROCESSED
    assert move_action_for("duplicate") is MoveAction.PROCESSED
    assert move_action_for("failed") is MoveAction.FAILED
    assert move_action_for("retry") is MoveAction.LEAVE


def test_unknown_status_is_leave():
    # Contract skew / garbage → never move the file (DX-7).
    assert move_action_for("teleported") is MoveAction.LEAVE
    assert move_action_for("") is MoveAction.LEAVE