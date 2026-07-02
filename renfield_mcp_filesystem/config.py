"""Configuration: global settings from env + watch ROOTS from a mounted
``roots.yaml`` (so the YAML can be a ConfigMap and credentials stay in a Secret,
referenced by env-var NAME — DX-2). Roots are reloadable at runtime (T13) — the
loader is pure, so the daemon can re-read on a file-change without a redeploy.
"""

from __future__ import annotations

import os
from typing import Annotated, Literal, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

DEFAULT_EXTENSIONS = "pdf,docx,doc,txt,md,html,pptx,xlsx,png,jpg,jpeg"


class SmbCredentials(BaseModel):
    username: str
    password: str
    domain: str | None = None


class _RootBase(BaseModel):
    name: str
    # Subdirs (relative to the root) the watcher moves files into by the
    # 4-state response. MUST be on the same filesystem as the root (EXDEV-safe)
    # and are themselves ignored by the watcher (never re-ingested).
    processed_subdir: str = "processed"
    failed_subdir: str = "failed"

    @field_validator("name")
    @classmethod
    def _name_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("root name must be non-empty")
        return v.strip()

    @model_validator(mode="after")
    def _subdirs_distinct(self):
        if self.processed_subdir == self.failed_subdir:
            raise ValueError("processed_subdir and failed_subdir must differ")
        return self


class LocalRoot(_RootBase):
    type: Literal["local"] = "local"
    path: str  # absolute path to the watched inbox directory


class SmbRoot(_RootBase):
    type: Literal["smb"] = "smb"
    server: str
    share: str
    path: str = ""  # subdir within the share (the watched inbox); "" = share root
    port: int = 445
    # Credentials referenced by ENV-VAR NAME, never inlined (DX-2).
    username_env: str
    password_env: str
    domain_env: str | None = None

    def credentials(self) -> SmbCredentials:
        """Resolve the referenced env vars at use time. Raises if a referenced
        var is unset — fail loud rather than connect anonymously."""
        username = os.environ.get(self.username_env)
        password = os.environ.get(self.password_env)
        if not username or not password:
            missing = [
                e for e, v in ((self.username_env, username), (self.password_env, password))
                if not v
            ]
            raise ValueError(
                f"SMB root {self.name!r}: credential env var(s) unset: {missing}"
            )
        domain = os.environ.get(self.domain_env) if self.domain_env else None
        return SmbCredentials(username=username, password=password, domain=domain)


Root = Annotated[Union[LocalRoot, SmbRoot], Field(discriminator="type")]


class RootsFile(BaseModel):
    roots: list[Root] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_names(self):
        names = [r.name for r in self.roots]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate root names: {sorted(dupes)}")
        return self


class Config(BaseModel):
    renfield_url: str
    ingest_token: str
    allowed_extensions: tuple[str, ...]
    settle_seconds: float = 2.0
    max_file_size_mb: int = 50
    push_timeout_seconds: float = 120.0
    roots_path: str | None = None  # the mounted roots.yaml (for reload)
    roots: list[Root] = Field(default_factory=list)
    # Bound concurrent pushes across ALL roots + retries so a large first-run
    # backlog (or a retry storm during a backend slowdown) can't fan out into a
    # flood of simultaneous ingest requests. A defense-in-depth cap: the backend
    # is the authority on its own load, but the MCP shouldn't be the source of a
    # thundering herd. Shared by every engine via the daemon.
    max_concurrent_pushes: int = 4
    # Backend health poll (recovery detector, NOT a filesystem poll). On a
    # down→up transition the daemon re-reconciles every root so files left in the
    # inbox after retry-exhaustion during a backend outage/restart are re-tried
    # WITHOUT a manual MCP restart. 0 disables the poller.
    health_poll_seconds: float = 30.0

    @property
    def max_file_size_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

    def extension_allowed(self, filename: str) -> bool:
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        return bool(ext) and ext in self.allowed_extensions

    def root_by_name(self, name: str) -> Root | None:
        return next((r for r in self.roots if r.name == name), None)


def _parse_extensions(raw: str) -> tuple[str, ...]:
    return tuple(e.strip().lower() for e in raw.split(",") if e.strip())


def load_roots(roots_path: str) -> list[Root]:
    """Parse + validate the roots YAML. Raises on malformed config (fail loud at
    startup / reload rather than watch nothing silently)."""
    with open(roots_path) as f:
        data = yaml.safe_load(f) or {}
    return RootsFile.model_validate(data).roots


def load_config() -> Config:
    """Build the Config from env (+ the roots YAML if ``FILES_ROOTS_YAML`` is set)."""
    renfield_url = os.environ.get("RENFIELD_URL", "").rstrip("/")
    if not renfield_url:
        raise ValueError("RENFIELD_URL is required")
    ingest_token = os.environ.get("RENFIELD_INGEST_TOKEN", "")
    if not ingest_token:
        raise ValueError("RENFIELD_INGEST_TOKEN is required")

    roots_path = os.environ.get("FILES_ROOTS_YAML") or None
    roots = load_roots(roots_path) if roots_path else []

    return Config(
        renfield_url=renfield_url,
        ingest_token=ingest_token,
        allowed_extensions=_parse_extensions(
            os.environ.get("FILES_ALLOWED_EXTENSIONS", DEFAULT_EXTENSIONS)
        ),
        settle_seconds=float(os.environ.get("FILES_SETTLE_SECONDS", "2.0")),
        max_file_size_mb=int(os.environ.get("FILES_MAX_FILE_SIZE_MB", "50")),
        push_timeout_seconds=float(os.environ.get("FILES_PUSH_TIMEOUT_SECONDS", "120")),
        max_concurrent_pushes=int(os.environ.get("FILES_MAX_CONCURRENT_PUSHES", "4")),
        health_poll_seconds=float(os.environ.get("FILES_HEALTH_POLL_SECONDS", "30")),
        roots_path=roots_path,
        roots=roots,
    )
