import types

import pytest

from renfield_mcp_filesystem.config import LocalRoot
from renfield_mcp_filesystem.providers.local import LocalProvider, _InboxHandler


# -- handler event translation (event-driven, no FS) --

def _handler(inbox):
    emitted = []
    h = _InboxHandler(inbox, {"processed", "failed"}, emitted.append)
    return h, emitted


def test_handler_on_closed_emits_toplevel_file(tmp_path):
    h, emitted = _handler(tmp_path)
    h.on_closed(types.SimpleNamespace(src_path=str(tmp_path / "a.pdf"), is_directory=False))
    assert emitted == ["a.pdf"]


def test_handler_on_moved_emits_dest(tmp_path):
    h, emitted = _handler(tmp_path)
    h.on_moved(types.SimpleNamespace(
        src_path="/elsewhere/x.pdf", dest_path=str(tmp_path / "x.pdf"), is_directory=False
    ))
    assert emitted == ["x.pdf"]


def test_handler_ignores_directories_subdirs_and_nested(tmp_path):
    h, emitted = _handler(tmp_path)
    # directory event
    h.on_closed(types.SimpleNamespace(src_path=str(tmp_path / "d"), is_directory=True))
    # the processed/ subdir name (a move-out destination would be nested anyway)
    h.on_closed(types.SimpleNamespace(src_path=str(tmp_path / "processed"), is_directory=False))
    # nested file (e.g. moved into processed/) — parent != inbox
    h.on_moved(types.SimpleNamespace(
        src_path=str(tmp_path / "a.pdf"),
        dest_path=str(tmp_path / "processed" / "a.pdf"),
        is_directory=False,
    ))
    assert emitted == []


# -- on-demand file ops (real tmp files) --

def _provider(tmp_path):
    return LocalProvider(LocalRoot(name="r", path=str(tmp_path)))


@pytest.mark.asyncio
async def test_stat_and_read(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"hello")
    p = _provider(tmp_path)
    info = await p.stat("a.pdf")
    assert info is not None and info.size == 5 and info.relpath == "a.pdf"
    assert await p.read_bytes("a.pdf") == b"hello"
    assert await p.stat("missing.pdf") is None


@pytest.mark.asyncio
async def test_move_to_subdir_and_collision(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"1")
    p = _provider(tmp_path)
    new = await p.move_to_subdir("a.pdf", "processed")
    assert new == "processed/a.pdf"
    assert (tmp_path / "processed" / "a.pdf").read_bytes() == b"1"
    assert not (tmp_path / "a.pdf").exists()
    # collision → unique suffix
    (tmp_path / "a.pdf").write_bytes(b"2")
    new2 = await p.move_to_subdir("a.pdf", "processed")
    assert new2 == "processed/a_1.pdf"
    assert (tmp_path / "processed" / "a_1.pdf").read_bytes() == b"2"


@pytest.mark.asyncio
async def test_list_files_excludes_subdirs(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"x")
    (tmp_path / "b.txt").write_bytes(b"y")
    (tmp_path / "processed").mkdir()
    (tmp_path / "processed" / "old.pdf").write_bytes(b"z")
    p = _provider(tmp_path)
    names = [fi.relpath for fi in await p.list_files()]
    assert names == ["a.pdf", "b.txt"]  # sorted, no subdir contents
    pdfs = [fi.relpath for fi in await p.list_files("*.pdf")]
    assert pdfs == ["a.pdf"]


@pytest.mark.asyncio
async def test_traversal_rejected(tmp_path):
    p = _provider(tmp_path)
    with pytest.raises(ValueError, match="traversal"):
        await p.stat("../../etc/passwd")
    with pytest.raises(ValueError, match="traversal"):
        await p.read_bytes("../escape.pdf")


# -- rename_within_processed (#881) --

@pytest.mark.asyncio
async def test_rename_within_processed_renames_and_keeps_ext(tmp_path):
    proc = tmp_path / "processed"
    proc.mkdir()
    (proc / "2026_03_29.pdf").write_bytes(b"pdfbytes")
    p = _provider(tmp_path)
    new = await p.rename_within_processed("2026_03_29.pdf", "Rechnung PVS vom 18.05.2026.pdf")
    assert new == "processed/Rechnung PVS vom 18.05.2026.pdf"
    assert (proc / "Rechnung PVS vom 18.05.2026.pdf").read_bytes() == b"pdfbytes"
    assert not (proc / "2026_03_29.pdf").exists()


@pytest.mark.asyncio
async def test_rename_within_processed_noop_when_source_absent(tmp_path):
    (tmp_path / "processed").mkdir()
    p = _provider(tmp_path)
    # Source never moved / already renamed → idempotent no-op (None), no raise.
    assert await p.rename_within_processed("gone.pdf", "whatever.pdf") is None


@pytest.mark.asyncio
async def test_rename_within_processed_collision_suffix(tmp_path):
    proc = tmp_path / "processed"
    proc.mkdir()
    (proc / "src.pdf").write_bytes(b"new")
    (proc / "Invoice.pdf").write_bytes(b"existing")  # target already taken
    p = _provider(tmp_path)
    new = await p.rename_within_processed("src.pdf", "Invoice.pdf")
    assert new == "processed/Invoice (2).pdf"
    assert (proc / "Invoice (2).pdf").read_bytes() == b"new"
    assert (proc / "Invoice.pdf").read_bytes() == b"existing"  # untouched


@pytest.mark.asyncio
async def test_rename_within_processed_same_name_is_noop(tmp_path):
    proc = tmp_path / "processed"
    proc.mkdir()
    (proc / "same.pdf").write_bytes(b"x")
    p = _provider(tmp_path)
    new = await p.rename_within_processed("same.pdf", "same.pdf")
    assert new == "processed/same.pdf"
    assert (proc / "same.pdf").read_bytes() == b"x"


@pytest.mark.asyncio
async def test_rename_within_processed_rejects_traversal(tmp_path):
    p = _provider(tmp_path)
    with pytest.raises(ValueError):
        await p.rename_within_processed("../escape.pdf", "x.pdf")
    with pytest.raises(ValueError):
        await p.rename_within_processed("ok.pdf", "../../evil.pdf")
