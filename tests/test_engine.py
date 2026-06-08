import asyncio
from collections.abc import AsyncIterator

import pytest

from renfield_mcp_filesystem.config import Config, LocalRoot
from renfield_mcp_filesystem.contract import MoveAction
from renfield_mcp_filesystem.engine import IngestEngine
from renfield_mcp_filesystem.providers.base import FileInfo, FolderProvider, SettledFile
from renfield_mcp_filesystem.pusher import PushOutcome


class FakeProvider(FolderProvider):
    def __init__(self, files, *, existing=(), events=()):
        super().__init__("r")
        self.files = dict(files)  # relpath -> bytes
        self._existing = list(existing)
        self._events = list(events)
        self.moved = []  # (relpath, subdir)
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        pass

    async def watch(self) -> AsyncIterator[SettledFile]:
        for r in self._events:
            yield SettledFile(r)

    async def stat(self, relpath):
        if relpath in self.files:
            return FileInfo(relpath, len(self.files[relpath]), 0.0)
        return None

    async def read_bytes(self, relpath):
        return self.files[relpath]

    async def move_to_subdir(self, relpath, subdir):
        self.moved.append((relpath, subdir))
        self.files.pop(relpath, None)
        return f"{subdir}/{relpath}"

    async def list_files(self, pattern=None):
        return [FileInfo(r, len(self.files[r]), 0.0) for r in self._existing if r in self.files]

    @property
    def connected(self):
        return True


class FakePusher:
    def __init__(self, outcome):
        self._outcome = outcome
        self.calls = []

    async def push(self, **kwargs):
        self.calls.append(kwargs)
        return self._outcome(kwargs) if callable(self._outcome) else self._outcome


def _cfg(max_mb=50):
    return Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf", "txt"), max_file_size_mb=max_mb,
    )


def _engine(provider, pusher, *, on_failed=None, on_fatal=None, max_retries=3, retry_base=0.001):
    return IngestEngine(
        config=_cfg(), root=LocalRoot(name="r", path="/watch"), provider=provider,
        pusher=pusher, on_failed=on_failed, on_fatal=on_fatal,
        max_retries=max_retries, retry_base_seconds=retry_base,
    )


@pytest.mark.asyncio
async def test_ingested_moves_to_processed():
    prov = FakeProvider({"a.pdf": b"%PDF"})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED, status="ingested", document_id=5))
    eng = _engine(prov, push)
    await eng._dispatch("a.pdf")
    assert prov.moved == [("a.pdf", "processed")]
    assert len(push.calls) == 1
    # pushed with the right sha + filename
    assert push.calls[0]["filename"] == "a.pdf" and len(push.calls[0]["sha256"]) == 64


@pytest.mark.asyncio
async def test_failed_status_moves_to_failed():
    prov = FakeProvider({"a.pdf": b"%PDF"})
    eng = _engine(prov, FakePusher(PushOutcome(move=MoveAction.FAILED, status="failed", detail="x")))
    await eng._dispatch("a.pdf")
    assert prov.moved == [("a.pdf", "failed")]


@pytest.mark.asyncio
async def test_oversize_moves_to_failed_without_push():
    # A >1 MB file against a 1 MB ceiling → failed, never pushed (security gate).
    prov = FakeProvider({"big.pdf": b"x" * (1024 * 1024 + 10)})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = IngestEngine(
        config=Config(renfield_url="http://x", ingest_token="t",
                      allowed_extensions=("pdf",), max_file_size_mb=1),
        root=LocalRoot(name="r", path="/w"), provider=prov, pusher=push,
    )
    await eng._dispatch("big.pdf")
    assert prov.moved == [("big.pdf", "failed")]
    assert push.calls == []  # never pushed


@pytest.mark.asyncio
async def test_empty_file_moves_to_failed_without_push():
    prov = FakeProvider({"e.pdf": b""})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng._dispatch("e.pdf")
    assert prov.moved == [("e.pdf", "failed")]
    assert push.calls == []


@pytest.mark.asyncio
async def test_bad_extension_moves_to_failed_without_push():
    prov = FakeProvider({"m.exe": b"MZxx"})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng._dispatch("m.exe")
    assert prov.moved == [("m.exe", "failed")]
    assert push.calls == []


@pytest.mark.asyncio
async def test_create_only_dedup_same_path_processed_once():
    prov = FakeProvider({"a.pdf": b"%PDF"})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng._dispatch("a.pdf")
    await eng._dispatch("a.pdf")  # second event for same path — ignored (already moved)
    assert len(push.calls) == 1 and prov.moved == [("a.pdf", "processed")]


@pytest.mark.asyncio
async def test_retry_then_exhausts_and_notifies():
    prov = FakeProvider({"a.pdf": b"%PDF"})
    push = FakePusher(PushOutcome(move=MoveAction.LEAVE))  # always retry
    failures = []

    async def on_failed(root, relpath, reason):
        failures.append((root, relpath, reason))

    eng = _engine(prov, push, on_failed=on_failed, max_retries=3, retry_base=0.001)
    await eng._dispatch("a.pdf")
    await asyncio.sleep(0.05)  # let the bounded backoff retries run
    assert len(push.calls) == 3  # attempt 0 + 2 retries (cap = max_retries)
    assert prov.moved == []  # never moved — left in inbox
    assert failures and "retry_exhausted" in failures[0][2]


@pytest.mark.asyncio
async def test_fatal_stops_root_and_notifies():
    prov = FakeProvider({"a.pdf": b"%PDF"})
    push = FakePusher(PushOutcome(move=MoveAction.LEAVE, fatal=True, http_status=403))
    fatals = []

    async def on_fatal(root, reason):
        fatals.append((root, reason))

    eng = _engine(prov, push, on_fatal=on_fatal)
    await eng._dispatch("a.pdf")
    assert fatals == [("r", "http_403")]
    assert prov.moved == []  # fatal → file untouched


@pytest.mark.asyncio
async def test_startup_reconciliation_dispatches_existing():
    prov = FakeProvider({"old.pdf": b"%PDF"}, existing=["old.pdf"], events=[])
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng.run()  # start → reconcile existing → watch (no events) → return
    assert prov.started is True
    assert prov.moved == [("old.pdf", "processed")]


@pytest.mark.asyncio
async def test_run_processes_watch_events():
    prov = FakeProvider({"a.pdf": b"%PDF", "b.pdf": b"%PDF2"}, events=["a.pdf", "b.pdf"])
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng.run()
    assert sorted(prov.moved) == [("a.pdf", "processed"), ("b.pdf", "processed")]
