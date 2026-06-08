"""Live local-watch integration test — exercises the REAL event-driven path
(watchdog → inotify CLOSE_WRITE → engine → gate → move), no mocking of the watch
mechanism. inotify CLOSE_WRITE is Linux-only, so this is skipped off Linux and
runs on the .159 build box.
"""

import asyncio
import sys

import pytest

from renfield_mcp_filesystem.config import Config, LocalRoot
from renfield_mcp_filesystem.contract import MoveAction
from renfield_mcp_filesystem.engine import IngestEngine
from renfield_mcp_filesystem.providers.local import LocalProvider
from renfield_mcp_filesystem.pusher import PushOutcome

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="inotify CLOSE_WRITE is Linux-only"
)


class _RecordingPusher:
    def __init__(self, outcome):
        self.outcome = outcome
        self.pushes = []

    async def push(self, **kwargs):
        self.pushes.append(kwargs)
        return self.outcome


async def _wait_for(predicate, timeout=8.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.mark.asyncio
async def test_live_local_detects_settle_and_moves(tmp_path):
    cfg = Config(renfield_url="http://x", ingest_token="t", allowed_extensions=("pdf",))
    root = LocalRoot(name="r", path=str(tmp_path))
    provider = LocalProvider(root)
    pusher = _RecordingPusher(PushOutcome(move=MoveAction.PROCESSED, status="ingested", document_id=1))
    engine = IngestEngine(config=cfg, root=root, provider=provider, pusher=pusher)

    run_task = asyncio.create_task(engine.run())
    await asyncio.sleep(0.5)  # let the observer start

    # Write a file → its CLOSE_WRITE is the settle signal (event-driven).
    (tmp_path / "invoice.pdf").write_bytes(b"%PDF-1.4 live bytes")

    moved = await _wait_for(lambda: (tmp_path / "processed" / "invoice.pdf").exists())
    assert moved, "file was not detected + moved (live inotify path)"
    assert len(pusher.pushes) == 1
    assert pusher.pushes[0]["filename"] == "invoice.pdf"
    assert not (tmp_path / "invoice.pdf").exists()  # moved out of the inbox

    await engine.stop()
    run_task.cancel()


@pytest.mark.asyncio
async def test_live_local_rejects_bad_extension(tmp_path):
    cfg = Config(renfield_url="http://x", ingest_token="t", allowed_extensions=("pdf",))
    root = LocalRoot(name="r", path=str(tmp_path))
    provider = LocalProvider(root)
    pusher = _RecordingPusher(PushOutcome(move=MoveAction.PROCESSED))
    engine = IngestEngine(config=cfg, root=root, provider=provider, pusher=pusher)

    run_task = asyncio.create_task(engine.run())
    await asyncio.sleep(0.5)
    (tmp_path / "malware.exe").write_bytes(b"MZ live")

    moved = await _wait_for(lambda: (tmp_path / "failed" / "malware.exe").exists())
    assert moved, "bad-extension file was not moved to failed/"
    assert pusher.pushes == []  # never pushed (local gate)

    await engine.stop()
    run_task.cancel()
