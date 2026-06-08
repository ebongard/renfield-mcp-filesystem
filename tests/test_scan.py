import pytest

from renfield_mcp_filesystem.config import Config, LocalRoot, SmbRoot
from renfield_mcp_filesystem.gate import classify
from renfield_mcp_filesystem.scan import dry_run, format_report


def _cfg(roots, max_mb=1):
    return Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf", "txt"), max_file_size_mb=max_mb, roots=roots,
    )


def test_gate_classify():
    cfg = _cfg([], max_mb=1)
    assert classify(cfg, "a.pdf", 100) == (True, None)
    assert classify(cfg, "a.pdf", 0) == (False, "empty")
    assert classify(cfg, "a.pdf", 2 * 1024 * 1024) == (False, "oversize")
    assert classify(cfg, "a.exe", 100) == (False, "extension_not_allowed")


@pytest.mark.asyncio
async def test_dry_run_classifies_and_moves_nothing(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"%PDF ok")
    (tmp_path / "big.pdf").write_bytes(b"x" * (1024 * 1024 + 10))  # >1MB
    (tmp_path / "m.exe").write_bytes(b"MZ")
    (tmp_path / "e.pdf").write_bytes(b"")
    cfg = _cfg([LocalRoot(name="docs", path=str(tmp_path))], max_mb=1)

    reports = await dry_run(cfg)
    assert len(reports) == 1
    r = reports[0]
    assert r.error is None
    assert [m[0] for m in r.matched] == ["a.pdf"]
    skipped = dict(r.skipped)
    assert skipped == {"big.pdf": "oversize", "e.pdf": "empty", "m.exe": "extension_not_allowed"}

    # nothing moved/pushed — all four still in the inbox top-level
    assert {p.name for p in tmp_path.iterdir() if p.is_file()} == {"a.pdf", "big.pdf", "m.exe", "e.pdf"}


@pytest.mark.asyncio
async def test_dry_run_reports_connect_error(monkeypatch, tmp_path):
    # An SMB root whose credential env vars are unset → connect() raises → the
    # error is reported per-root (surfaces a bad credential at preflight).
    monkeypatch.delenv("NOPE_U", raising=False)
    monkeypatch.delenv("NOPE_P", raising=False)
    cfg = _cfg([SmbRoot(name="nas", server="s", share="S",
                        username_env="NOPE_U", password_env="NOPE_P")])
    reports = await dry_run(cfg)
    assert reports[0].error is not None and "credential" in reports[0].error


@pytest.mark.asyncio
async def test_format_report(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"%PDF")
    cfg = _cfg([LocalRoot(name="docs", path=str(tmp_path))])
    text = format_report(await dry_run(cfg))
    assert "DRY RUN" in text and "would push" in text and "a.pdf" in text
