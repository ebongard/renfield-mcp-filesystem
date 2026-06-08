import base64

import pytest

from renfield_mcp_filesystem import tools as t
from renfield_mcp_filesystem.config import Config, LocalRoot
from renfield_mcp_filesystem.registry import ProviderRegistry


def _registry(tmp_path):
    cfg = Config(
        renfield_url="http://renfield", ingest_token="tok",
        allowed_extensions=("pdf", "txt"),
        roots=[LocalRoot(name="docs", path=str(tmp_path))],
    )
    return ProviderRegistry(cfg)


@pytest.mark.asyncio
async def test_list_watch_folders(tmp_path):
    reg = _registry(tmp_path)
    out = await t.list_watch_folders(reg)
    assert out["roots"] == [{"name": "docs", "connected": False}]  # not started


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
async def test_traversal_via_path_rejected(tmp_path):
    reg = _registry(tmp_path)
    # split_path yields relpath "../../etc/passwd"; the provider rejects traversal
    out = await t.read_file(reg, "docs/../../etc/passwd")
    assert "error" in out  # provider raises → surfaced as not-found/error
