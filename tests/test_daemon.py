"""Dynamic-root management (T13): the DaemonManager diffs new roots against the
running set and starts/stops/rebuilds only what changed."""

import asyncio
import textwrap

import pytest

from renfield_mcp_filesystem.config import Config, LocalRoot, load_roots
from renfield_mcp_filesystem.daemon import DaemonManager
from renfield_mcp_filesystem.providers.base import FolderProvider


class FakeProv(FolderProvider):
    def __init__(self, root, settle_seconds=2.0):
        super().__init__(root.name)
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def watch(self):
        return
        yield  # noqa - makes this an (empty) async generator

    async def stat(self, relpath):
        return None

    async def read_bytes(self, relpath):
        return b""

    async def move_to_subdir(self, relpath, subdir):
        return f"{subdir}/{relpath}"

    async def list_files(self, pattern=None):
        return []

    async def rename_within_processed(self, src_name, target_name):
        return None

    @property
    def connected(self):
        return self.started and not self.stopped


def _factory(created):
    def make(root, settle_seconds=2.0):
        p = FakeProv(root)
        created.setdefault(root.name, []).append(p)
        return p
    return make


def _cfg(roots, roots_path=None):
    return Config(
        renfield_url="http://x", ingest_token="t",
        allowed_extensions=("pdf",), roots=roots, roots_path=roots_path,
    )


@pytest.mark.asyncio
async def test_apply_roots_add_remove_change():
    created: dict[str, list[FakeProv]] = {}
    mgr = DaemonManager(_cfg([LocalRoot(name="a", path="/a")]), provider_factory=_factory(created))
    assert mgr.names() == ["a"]  # built eagerly in __init__
    await mgr.start()
    await asyncio.sleep(0.01)
    assert created["a"][0].started

    # add b
    await mgr._apply_roots([LocalRoot(name="a", path="/a"), LocalRoot(name="b", path="/b")])
    await asyncio.sleep(0.01)
    assert sorted(mgr.names()) == ["a", "b"]
    assert created["b"][0].started

    # remove a
    await mgr._apply_roots([LocalRoot(name="b", path="/b")])
    assert mgr.names() == ["b"]
    assert created["a"][0].stopped is True

    # change b (different path) → rebuilt as a fresh provider
    await mgr._apply_roots([LocalRoot(name="b", path="/b2")])
    await asyncio.sleep(0.01)
    assert created["b"][0].stopped is True
    assert len(created["b"]) == 2 and created["b"][1].started

    await mgr.stop()


@pytest.mark.asyncio
async def test_reload_from_file_and_bad_yaml_keeps_current(tmp_path):
    yaml = tmp_path / "roots.yaml"
    yaml.write_text(textwrap.dedent("""
        roots:
          - {name: a, type: local, path: /a}
    """))
    created: dict[str, list[FakeProv]] = {}
    cfg = _cfg(load_roots(str(yaml)), roots_path=str(yaml))
    mgr = DaemonManager(cfg, provider_factory=_factory(created))

    # add b via the file → reload picks it up
    yaml.write_text(textwrap.dedent("""
        roots:
          - {name: a, type: local, path: /a}
          - {name: b, type: local, path: /b}
    """))
    await mgr.reload()
    assert sorted(mgr.names()) == ["a", "b"]

    # malformed YAML → reload logs + keeps the current roots
    yaml.write_text("roots: [unterminated")
    await mgr.reload()
    assert sorted(mgr.names()) == ["a", "b"]

    await mgr.stop()


@pytest.mark.asyncio
async def test_health_recovery_triggers_reconcile(monkeypatch):
    # On a backend down→up transition the daemon re-reconciles every root
    # (un-sticks files left after retry-exhaustion during the outage) WITHOUT
    # a restart.
    created: dict[str, list[FakeProv]] = {}
    cfg = _cfg([LocalRoot(name="a", path="/a")])
    object.__setattr__(cfg, "health_poll_seconds", 0.01)
    mgr = DaemonManager(cfg, provider_factory=_factory(created))

    reconciled = {"count": 0}

    async def fake_reconcile():
        reconciled["count"] += 1

    for eng in mgr._engines.values():
        eng.reconcile = fake_reconcile

    # Health flips: down, then up (→ should reconcile once on the up edge).
    seq = iter([False, True, True])

    async def fake_health():
        try:
            return next(seq)
        except StopIteration:
            return True

    mgr._pusher.health = fake_health

    await mgr.start()
    await asyncio.sleep(0.05)  # allow a few poll ticks
    await mgr.stop()
    assert reconciled["count"] >= 1  # reconciled on the down→up edge


@pytest.mark.asyncio
async def test_health_stable_up_does_not_reconcile(monkeypatch):
    # Steady-healthy backend must NOT trigger repeated reconciles (only the
    # down→up edge does).
    created: dict[str, list[FakeProv]] = {}
    cfg = _cfg([LocalRoot(name="a", path="/a")])
    object.__setattr__(cfg, "health_poll_seconds", 0.01)
    mgr = DaemonManager(cfg, provider_factory=_factory(created))

    reconciled = {"count": 0}

    async def fake_reconcile():
        reconciled["count"] += 1

    for eng in mgr._engines.values():
        eng.reconcile = fake_reconcile

    async def always_up():
        return True

    mgr._pusher.health = always_up
    await mgr.start()
    await asyncio.sleep(0.05)
    await mgr.stop()
    assert reconciled["count"] == 0  # never dipped → no recovery reconcile
