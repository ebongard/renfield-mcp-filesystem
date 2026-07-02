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


def _engine(provider, pusher, *, on_failed=None, on_fatal=None, max_retries=3,
            retry_base=0.001, backend_retry_max=None, empty_retry_max=None):
    # In tests the differentiated budgets default to max_retries (so a single
    # small cap keeps the async retry loops short) unless a test overrides them.
    return IngestEngine(
        config=_cfg(), root=LocalRoot(name="r", path="/watch"), provider=provider,
        pusher=pusher, on_failed=on_failed, on_fatal=on_fatal,
        max_retries=max_retries, retry_base_seconds=retry_base,
        backend_retry_max=max_retries if backend_retry_max is None else backend_retry_max,
        empty_retry_max=max_retries if empty_retry_max is None else empty_retry_max,
        retry_max_delay=0.001,
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
async def test_empty_file_retries_then_fails():
    # 0 bytes at settle = likely a mid-copy read (SMB has no CLOSE_WRITE). The
    # engine retries a few times to let the copy finish; a file that stays empty
    # across the budget is genuinely empty → failed/ (never pushed).
    prov = FakeProvider({"e.pdf": b""})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push, empty_retry_max=3)
    await eng._dispatch("e.pdf")
    await asyncio.sleep(0.05)  # let the empty-wait retries run
    assert prov.moved == [("e.pdf", "failed")]
    assert push.calls == []  # never pushed an empty file


@pytest.mark.asyncio
async def test_empty_then_populated_is_pushed():
    # A file that is 0 bytes at first stat but has content by a later retry (the
    # copy finished) gets pushed, not failed.
    prov = FakeProvider({"e.pdf": b""})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED, status="ingested"))
    eng = _engine(prov, push, empty_retry_max=5)
    await eng._dispatch("e.pdf")
    prov.files["e.pdf"] = b"%PDF now has bytes"  # copy completes after first check
    await asyncio.sleep(0.05)
    assert prov.moved == [("e.pdf", "processed")]
    assert len(push.calls) == 1


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
    assert len(push.calls) == 3  # attempt 0 + 2 retries (cap = backend_retry_max)
    assert prov.moved == []  # never moved — left in inbox
    assert failures and "retry_exhausted" in failures[0][2]


@pytest.mark.asyncio
async def test_backend_retry_uses_larger_budget_than_transient_cap():
    # A LEAVE = backend RETRY (doc still OCR'ing). It must use the LARGE backend
    # budget, not the small transient-error cap, so a minutes-long inbox backlog
    # can't strand the file. Here backend_retry_max=6 > max_retries=2.
    prov = FakeProvider({"a.pdf": b"%PDF"})
    push = FakePusher(PushOutcome(move=MoveAction.LEAVE))  # always retry
    failures = []

    async def on_failed(root, relpath, reason):
        failures.append((root, relpath, reason))

    eng = _engine(prov, push, on_failed=on_failed, max_retries=2, backend_retry_max=6)
    await eng._dispatch("a.pdf")
    await asyncio.sleep(0.1)
    assert len(push.calls) == 6  # used backend_retry_max=6, not max_retries=2
    assert prov.moved == []  # left in inbox for the next reconcile
    assert failures and "retry_exhausted: retry" in failures[0][2]


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


@pytest.mark.asyncio
async def test_dotfiles_are_ignored():
    prov = FakeProvider({".DS_Store": b"junk", "real.pdf": b"%PDF"})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng._dispatch(".DS_Store")   # ignored — not processed/moved
    await eng._dispatch("real.pdf")
    assert prov.moved == [("real.pdf", "processed")]
    assert len(push.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["", "   ", "/", "\\", " / "])
async def test_empty_or_dir_level_event_never_moves_the_inbox(bad):
    # CRITICAL regression: an empty/whitespace/slash-only relpath resolves to the
    # inbox dir itself; dispatching it must NOT push or move anything (a move
    # would relocate the entire inbox into failed/).
    prov = FakeProvider({"real.pdf": b"%PDF"})
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng._dispatch(bad)
    assert prov.moved == []
    assert push.calls == []


@pytest.mark.asyncio
async def test_reconcile_redispatches_inbox_files():
    # Public reconcile() re-runs the one-shot inbox catch-up on demand (the
    # daemon calls it on backend recovery). A file left in the inbox is pushed.
    prov = FakeProvider({"left.pdf": b"%PDF"}, existing=["left.pdf"])
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED, status="ingested"))
    eng = _engine(prov, push)
    await prov.start()
    await eng.reconcile()
    assert prov.moved == [("left.pdf", "processed")]


@pytest.mark.asyncio
async def test_reconcile_noop_when_stopped():
    # A fatally-stopped root (token error) must NOT be re-reconciled — that needs
    # operator action, not an auto retry.
    prov = FakeProvider({"left.pdf": b"%PDF"}, existing=["left.pdf"])
    push = FakePusher(PushOutcome(move=MoveAction.PROCESSED))
    eng = _engine(prov, push)
    await eng.stop()  # sets _stopped
    await eng.reconcile()
    assert prov.moved == []
    assert push.calls == []


@pytest.mark.asyncio
async def test_push_semaphore_bounds_concurrency():
    # With a shared semaphore of 1, two concurrent dispatches must not push at
    # the same time (the flood-prevention property).
    import asyncio

    sem = asyncio.Semaphore(1)
    concurrent = 0
    peak = 0

    class SlowPusher:
        calls = []

        async def push(self, **kwargs):
            nonlocal concurrent, peak
            concurrent += 1
            peak = max(peak, concurrent)
            await asyncio.sleep(0.02)
            concurrent -= 1
            SlowPusher.calls.append(kwargs)
            return PushOutcome(move=MoveAction.PROCESSED)

    prov = FakeProvider({"a.pdf": b"%PDF", "b.pdf": b"%PDF"})
    eng = IngestEngine(
        config=_cfg(), root=LocalRoot(name="r", path="/watch"), provider=prov,
        pusher=SlowPusher(), push_semaphore=sem,
    )
    await asyncio.gather(eng._dispatch("a.pdf"), eng._dispatch("b.pdf"))
    assert peak == 1  # semaphore of 1 serialized the two pushes
    assert len(SlowPusher.calls) == 2
