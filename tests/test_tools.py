import base64

import pytest

from renfield_mcp_filesystem import tools as t
from renfield_mcp_filesystem.config import Config, LocalRoot
from renfield_mcp_filesystem.daemon import DaemonManager


def _registry(tmp_path):
    cfg = Config(
        renfield_url="http://renfield", ingest_token="tok",
        allowed_extensions=("pdf", "txt"),
        roots=[LocalRoot(name="docs", path=str(tmp_path))],
    )
    return DaemonManager(cfg)


@pytest.mark.asyncio
async def test_list_watch_folders(tmp_path):
    reg = _registry(tmp_path)
    out = await t.list_watch_folders(reg)
    assert out["roots"] == [
        {"name": "docs", "connected": False, "last_error": None}  # not started
    ]


@pytest.mark.asyncio
async def test_list_files_returns_qualified_paths(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"x")
    (tmp_path / "b.txt").write_bytes(b"yy")
    reg = _registry(tmp_path)
    out = await t.list_files(reg, "docs")
    assert out["root"] == "docs"
    paths = [f["path"] for f in out["files"]]
    assert paths == ["docs/a.pdf", "docs/b.txt"]
    assert out["files"][0]["size"] == 1


@pytest.mark.asyncio
async def test_list_files_unknown_root(tmp_path):
    out = await t.list_files(_registry(tmp_path), "nope")
    assert "error" in out


@pytest.mark.asyncio
async def test_get_file_info(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"hello")
    reg = _registry(tmp_path)
    out = await t.get_file_info(reg, "docs/a.pdf")
    assert out["size"] == 5 and out["relpath"] == "a.pdf"
    assert "error" in await t.get_file_info(reg, "docs/missing.pdf")
    assert "error" in await t.get_file_info(reg, "noslash")  # bad qualified path


@pytest.mark.asyncio
async def test_read_file_roundtrip_and_truncate(tmp_path):
    content = b"%PDF" + b"z" * 200_000
    (tmp_path / "big.pdf").write_bytes(content)
    reg = _registry(tmp_path)

    full = await t.read_file(reg, "docs/big.pdf", truncate=False)
    assert full["truncated"] is False
    assert base64.b64decode(full["content_base64"]) == content
    assert full["filename"] == "big.pdf" and full["size"] == len(content)

    capped = await t.read_file(reg, "docs/big.pdf", truncate=True)
    assert capped["truncated"] is True
    assert len(base64.b64decode(capped["content_base64"])) == t._TRUNCATE_BYTES


@pytest.mark.asyncio
async def test_read_file_unknown_root_and_missing(tmp_path):
    reg = _registry(tmp_path)
    assert "error" in await t.read_file(reg, "nope/a.pdf")
    assert "error" in await t.read_file(reg, "docs/missing.pdf")


@pytest.mark.asyncio
async def test_move_file(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"x")
    reg = _registry(tmp_path)
    out = await t.move_file(reg, "docs/a.pdf", "processed")
    assert out["moved_to"] == "docs/processed/a.pdf"
    assert (tmp_path / "processed" / "a.pdf").exists()


@pytest.mark.asyncio
async def test_move_file_invalid_subdir(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"x")
    reg = _registry(tmp_path)
    assert "error" in await t.move_file(reg, "docs/a.pdf", "../escape")
    assert "error" in await t.move_file(reg, "docs/a.pdf", "a/b")


@pytest.mark.asyncio
async def test_rename_processed_sanitizes_and_keeps_ext(tmp_path):
    proc = tmp_path / "processed"
    proc.mkdir()
    (proc / "2026_03_29.pdf").write_bytes(b"x")
    reg = _registry(tmp_path)
    # Illegal SMB chars (/ : ?) + collapsing whitespace; extension preserved.
    out = await t.rename_processed(reg, "2026_03_29.pdf", 'Rechnung: PVS/Rhein  vom 18.05.2026?')
    assert out["renamed"] is True and out["root"] == "docs"
    assert out["renamed_to"] == "docs/processed/Rechnung PVS Rhein vom 18.05.2026.pdf"
    assert (proc / "Rechnung PVS Rhein vom 18.05.2026.pdf").exists()


@pytest.mark.asyncio
async def test_rename_processed_idempotent_noop(tmp_path):
    (tmp_path / "processed").mkdir()
    reg = _registry(tmp_path)
    out = await t.rename_processed(reg, "never_moved.pdf", "Some Title")
    assert out["renamed"] is False and out["noop"] is True
    assert out["target"] == "Some Title.pdf"


@pytest.mark.asyncio
async def test_rename_processed_collision(tmp_path):
    proc = tmp_path / "processed"
    proc.mkdir()
    (proc / "src.pdf").write_bytes(b"new")
    (proc / "Title.pdf").write_bytes(b"old")
    reg = _registry(tmp_path)
    out = await t.rename_processed(reg, "src.pdf", "Title")
    assert out["renamed_to"] == "docs/processed/Title (2).pdf"


@pytest.mark.asyncio
async def test_rename_processed_rejects_traversal(tmp_path):
    reg = _registry(tmp_path)
    # original_name with a separator / traversal is a hard error (literal path part).
    assert "error" in await t.rename_processed(reg, "../etc/passwd", "x")
    assert "error" in await t.rename_processed(reg, "a/b.pdf", "x")
    # new_base that sanitizes to nothing usable is rejected.
    assert "error" in await t.rename_processed(reg, "ok.pdf", "///")
    assert "error" in await t.rename_processed(reg, "ok.pdf", "..")


@pytest.mark.asyncio
async def test_rename_processed_scrubs_separators_in_new_base(tmp_path):
    # A separator smuggled into new_base is SCRUBBED, never an escape.
    proc = tmp_path / "processed"
    proc.mkdir()
    (proc / "s.pdf").write_bytes(b"x")
    reg = _registry(tmp_path)
    out = await t.rename_processed(reg, "s.pdf", "../../evil")
    # "../../evil" → scrubbed to "evil"; stays inside processed/.
    assert out["renamed_to"] == "docs/processed/evil.pdf"
    assert (proc / "evil.pdf").exists()
    assert not (tmp_path.parent / "evil.pdf").exists()


@pytest.mark.asyncio
async def test_rename_processed_searches_all_roots(tmp_path):
    # Two roots; the file lives in the SECOND root's processed dir.
    r1 = tmp_path / "r1"
    r2 = tmp_path / "r2"
    (r2 / "processed").mkdir(parents=True)
    (r1 / "processed").mkdir(parents=True)
    (r2 / "processed" / "doc.pdf").write_bytes(b"z")
    cfg = Config(
        renfield_url="http://renfield", ingest_token="tok",
        allowed_extensions=("pdf",),
        roots=[LocalRoot(name="one", path=str(r1)), LocalRoot(name="two", path=str(r2))],
    )
    reg = DaemonManager(cfg)
    out = await t.rename_processed(reg, "doc.pdf", "Final Title")
    assert out["renamed"] is True and out["root"] == "two"
    assert (r2 / "processed" / "Final Title.pdf").exists()


@pytest.mark.asyncio
async def test_traversal_via_path_rejected(tmp_path):
    reg = _registry(tmp_path)
    # split_path yields relpath "../../etc/passwd"; the provider rejects traversal
    out = await t.read_file(reg, "docs/../../etc/passwd")
    assert "error" in out  # provider raises → surfaced as not-found/error
