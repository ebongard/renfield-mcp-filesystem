"""Security review H5 — path-traversal must not escape a watch root.

The SMB provider built UNC paths straight from a caller-supplied relpath,
preserving ``..`` for the SMB server to resolve outside the inbox (arbitrary
file read). Guard is now applied at the tool choke point (``split_path``) and
inside the SMB provider (``_child_unc``).
"""
import pytest

from renfield_mcp_filesystem import tools as t
from renfield_mcp_filesystem.config import Config, LocalRoot
from renfield_mcp_filesystem.daemon import DaemonManager
from renfield_mcp_filesystem.providers.base import safe_relpath


# ---- the shared guard ----

@pytest.mark.parametrize("bad", [
    "../etc/passwd",
    "../../etc/passwd",
    "a/../../b",
    r"..\..\windows\system32",
    "/etc/passwd",
    "\\\\nas\\share\\x",
    "C:/Windows",
    "file.txt:ads",          # NTFS alternate data stream
    "",
    "   ",
])
def test_safe_relpath_rejects(bad):
    with pytest.raises(ValueError):
        safe_relpath(bad)


@pytest.mark.parametrize("ok", [
    "file.pdf",
    "sub/file.pdf",
    r"sub\file.pdf",
    "deeply/nested/file.txt",
])
def test_safe_relpath_accepts(ok):
    assert safe_relpath(ok) == ok.strip()


# ---- the tool boundary (split_path) ----

def test_split_path_rejects_traversal():
    with pytest.raises(ValueError):
        t.split_path("docs/../../etc/passwd")


def test_split_path_accepts_normal():
    assert t.split_path("docs/sub/file.pdf") == ("docs", "sub/file.pdf")


def _registry(tmp_path):
    cfg = Config(
        renfield_url="http://renfield", ingest_token="tok",
        allowed_extensions=("pdf", "txt"),
        roots=[LocalRoot(name="docs", path=str(tmp_path))],
    )
    return DaemonManager(cfg)


@pytest.mark.asyncio
async def test_read_file_traversal_returns_error(tmp_path):
    # Plant a secret OUTSIDE the watched root.
    secret = tmp_path.parent / "secret.txt"
    secret.write_bytes(b"top secret")
    reg = _registry(tmp_path)

    out = await t.read_file(reg, "docs/../secret.txt")
    assert "error" in out
    assert "content_base64" not in out


@pytest.mark.asyncio
async def test_get_file_info_traversal_returns_error(tmp_path):
    reg = _registry(tmp_path)
    out = await t.get_file_info(reg, "docs/../../etc/passwd")
    assert "error" in out


@pytest.mark.asyncio
async def test_move_file_traversal_returns_error(tmp_path):
    reg = _registry(tmp_path)
    out = await t.move_file(reg, "docs/../x.pdf", "processed")
    assert "error" in out


# ---- SMB provider defense in depth ----

def test_smb_child_unc_rejects_traversal():
    from renfield_mcp_filesystem.config import SmbRoot
    from renfield_mcp_filesystem.providers.smb import SmbProvider

    p = SmbProvider(SmbRoot(name="r", server="nas", share="S",
                            username_env="U", password_env="P"))
    with pytest.raises(ValueError):
        p._child_unc("../../etc/passwd")
    # A normal relpath still builds a UNC under the inbox.
    assert p._child_unc("file.pdf").startswith(r"\\nas\S")
