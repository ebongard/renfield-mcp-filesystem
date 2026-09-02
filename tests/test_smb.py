"""Tests for the SMB provider's testable core — the settle debouncer, the
CHANGE_NOTIFY action classification, and UNC path building. The live SMB
connection + CHANGE_NOTIFY loop + file I/O are exercised by the .159 E2E.
"""

import asyncio

import pytest
from smbprotocol.change_notify import FileAction

from renfield_mcp_filesystem.config import SmbRoot
from renfield_mcp_filesystem.providers.smb import (
    SettleDebouncer,
    SmbProvider,
    _unc,
    classify_action,
)


def _smb_provider():
    return SmbProvider(
        SmbRoot(
            name="r", server="nas", share="Docs", path="incomming",
            username_env="U", password_env="P",
        )
    )


@pytest.mark.asyncio
async def test_rename_within_processed_rejects_bad_names():
    # The bare-name guard raises BEFORE any smbclient I/O (I/O is E2E-only).
    p = _smb_provider()
    with pytest.raises(ValueError):
        await p.rename_within_processed("../escape.pdf", "ok.pdf")
    with pytest.raises(ValueError):
        await p.rename_within_processed("ok.pdf", "sub/dir.pdf")


def test_classify_action():
    assert classify_action(FileAction.FILE_ACTION_ADDED) == "change"
    assert classify_action(FileAction.FILE_ACTION_MODIFIED) == "change"
    assert classify_action(FileAction.FILE_ACTION_RENAMED_NEW_NAME) == "change"
    assert classify_action(FileAction.FILE_ACTION_REMOVED) == "remove"
    assert classify_action(FileAction.FILE_ACTION_REMOVED_BY_DELETE) == "remove"
    assert classify_action(FileAction.FILE_ACTION_RENAMED_OLD_NAME) == "remove"
    assert classify_action(FileAction.FILE_ACTION_ADDED_STREAM) == "ignore"


def test_unc_building():
    assert _unc("nas", "Docs") == r"\\nas\Docs"
    assert _unc("nas", "Docs", "Inbox") == r"\\nas\Docs\Inbox"
    assert _unc("nas", "Docs", "Inbox", "a.pdf") == r"\\nas\Docs\Inbox\a.pdf"
    # empty / slash-only parts skipped; slashes normalised to backslashes
    assert _unc("nas", "Docs", "", "a.pdf") == r"\\nas\Docs\a.pdf"
    assert _unc("nas", "Docs", "sub/dir") == r"\\nas\Docs\sub\dir"


@pytest.mark.asyncio
async def test_debouncer_emits_after_settle():
    emitted = []
    d = SettleDebouncer(0.02, emitted.append)
    d.on_change("a.pdf")
    assert emitted == []  # not yet
    await asyncio.sleep(0.05)
    assert emitted == ["a.pdf"]


@pytest.mark.asyncio
async def test_debouncer_coalesces_rapid_changes():
    emitted = []
    d = SettleDebouncer(0.03, emitted.append)
    for _ in range(5):
        d.on_change("a.pdf")  # re-arm each time (still being written)
        await asyncio.sleep(0.01)
    assert emitted == []  # never quiet for a full settle window yet
    await asyncio.sleep(0.05)
    assert emitted == ["a.pdf"]  # exactly one emit after it goes quiet


@pytest.mark.asyncio
async def test_debouncer_remove_cancels():
    emitted = []
    d = SettleDebouncer(0.02, emitted.append)
    d.on_change("a.pdf")
    d.on_remove("a.pdf")
    await asyncio.sleep(0.05)
    assert emitted == []


@pytest.mark.asyncio
async def test_debouncer_independent_files():
    emitted = []
    d = SettleDebouncer(0.02, emitted.append)
    d.on_change("a.pdf")
    d.on_change("b.pdf")
    await asyncio.sleep(0.05)
    assert sorted(emitted) == ["a.pdf", "b.pdf"]


@pytest.mark.asyncio
async def test_debouncer_cancel_all():
    emitted = []
    d = SettleDebouncer(0.02, emitted.append)
    d.on_change("a.pdf")
    d.cancel_all()
    await asyncio.sleep(0.05)
    assert emitted == []


def test_reconnect_delay_backoff():
    from renfield_mcp_filesystem.providers.smb import reconnect_delay
    assert reconnect_delay(0, base=2, cap=60) == 2
    assert reconnect_delay(1, base=2, cap=60) == 4
    assert reconnect_delay(2, base=2, cap=60) == 8
    assert reconnect_delay(10, base=2, cap=60) == 60  # capped


def test_smb_last_error_default():
    from renfield_mcp_filesystem.config import SmbRoot
    from renfield_mcp_filesystem.providers.smb import SmbProvider
    p = SmbProvider(SmbRoot(name="r", server="nas", share="S",
                            username_env="U", password_env="P"))
    assert p.last_error is None and p.connected is False
