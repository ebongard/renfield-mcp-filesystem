import textwrap

import pytest
from pydantic import ValidationError

from renfield_mcp_filesystem.config import (
    Config,
    LocalRoot,
    SmbRoot,
    load_config,
    load_roots,
)


def _write(tmp_path, text):
    p = tmp_path / "roots.yaml"
    p.write_text(textwrap.dedent(text))
    return str(p)


def test_load_local_and_smb_roots(tmp_path):
    path = _write(tmp_path, """
        roots:
          - name: docs-local
            type: local
            path: /watch/docs
          - name: docs-smb
            type: smb
            server: nas.local
            share: Documents
            path: Inbox
            username_env: DOCS_SMB_USER
            password_env: DOCS_SMB_PASS
    """)
    roots = load_roots(path)
    assert len(roots) == 2
    assert isinstance(roots[0], LocalRoot) and roots[0].path == "/watch/docs"
    smb = roots[1]
    assert isinstance(smb, SmbRoot) and smb.server == "nas.local" and smb.port == 445
    # Defaults applied.
    assert roots[0].processed_subdir == "processed" and roots[0].failed_subdir == "failed"


def test_duplicate_root_names_rejected(tmp_path):
    path = _write(tmp_path, """
        roots:
          - {name: dup, type: local, path: /a}
          - {name: dup, type: local, path: /b}
    """)
    with pytest.raises(ValidationError):
        load_roots(path)


def test_subdirs_must_differ(tmp_path):
    path = _write(tmp_path, """
        roots:
          - {name: x, type: local, path: /a, processed_subdir: done, failed_subdir: done}
    """)
    with pytest.raises(ValidationError):
        load_roots(path)


def test_smb_credentials_from_env(monkeypatch):
    monkeypatch.setenv("MY_USER", "alice")
    monkeypatch.setenv("MY_PASS", "s3cret")
    root = SmbRoot(
        name="r", server="nas", share="S", username_env="MY_USER", password_env="MY_PASS"
    )
    creds = root.credentials()
    assert creds.username == "alice" and creds.password == "s3cret" and creds.domain is None


def test_smb_credentials_missing_env_raises(monkeypatch):
    monkeypatch.delenv("NOPE_USER", raising=False)
    monkeypatch.delenv("NOPE_PASS", raising=False)
    root = SmbRoot(
        name="r", server="nas", share="S", username_env="NOPE_USER", password_env="NOPE_PASS"
    )
    with pytest.raises(ValueError, match="credential env"):
        root.credentials()


def test_extension_and_size_helpers():
    cfg = Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf", "png"), max_file_size_mb=2,
    )
    assert cfg.extension_allowed("a.pdf") is True
    assert cfg.extension_allowed("a.PDF") is True
    assert cfg.extension_allowed("a.exe") is False
    assert cfg.extension_allowed("noext") is False
    assert cfg.max_file_size_bytes == 2 * 1024 * 1024


def test_load_config_requires_url_and_token(monkeypatch):
    monkeypatch.delenv("RENFIELD_URL", raising=False)
    with pytest.raises(ValueError, match="RENFIELD_URL"):
        load_config()
    monkeypatch.setenv("RENFIELD_URL", "http://renfield")
    monkeypatch.delenv("RENFIELD_INGEST_TOKEN", raising=False)
    with pytest.raises(ValueError, match="RENFIELD_INGEST_TOKEN"):
        load_config()


def test_load_config_from_env(monkeypatch, tmp_path):
    path = _write(tmp_path, """
        roots:
          - {name: local1, type: local, path: /watch}
    """)
    monkeypatch.setenv("RENFIELD_URL", "http://renfield/")
    monkeypatch.setenv("RENFIELD_INGEST_TOKEN", "tok")
    monkeypatch.setenv("FILES_ROOTS_YAML", path)
    monkeypatch.setenv("FILES_MAX_FILE_SIZE_MB", "10")
    monkeypatch.setenv("FILES_ALLOWED_EXTENSIONS", "pdf, txt")
    cfg = load_config()
    assert cfg.renfield_url == "http://renfield"  # trailing slash stripped
    assert cfg.max_file_size_mb == 10
    assert cfg.allowed_extensions == ("pdf", "txt")
    assert len(cfg.roots) == 1 and cfg.root_by_name("local1") is not None
