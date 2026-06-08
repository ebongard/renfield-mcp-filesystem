import json

import httpx
import pytest

import renfield_mcp_filesystem.pusher as pusher_mod
from renfield_mcp_filesystem.contract import CONTRACT_HEADER, MoveAction
from renfield_mcp_filesystem.pusher import RenfieldPusher


def _patch_transport(monkeypatch, handler):
    """Route the pusher's httpx.AsyncClient through a MockTransport."""
    real = httpx.AsyncClient

    def _factory(*args, **kwargs):
        kwargs.pop("timeout", None)
        return real(transport=httpx.MockTransport(handler), timeout=5)

    monkeypatch.setattr(pusher_mod.httpx, "AsyncClient", _factory)


def _pusher():
    return RenfieldPusher("http://renfield", "tok", timeout_seconds=5)


async def _push(p):
    return await p.push(
        file_bytes=b"%PDF", filename="a.pdf", root="r", relpath="a.pdf",
        sha256="deadbeef", mime="application/pdf",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [
    ("ingested", MoveAction.PROCESSED),
    ("duplicate", MoveAction.PROCESSED),
    ("failed", MoveAction.FAILED),
    ("retry", MoveAction.LEAVE),
])
async def test_200_status_maps_to_move(monkeypatch, status, expected):
    def handler(request):
        # request carries the bearer + contract header + multipart body
        assert request.headers["authorization"] == "Bearer tok"
        assert request.headers[CONTRACT_HEADER.lower()] == "1"
        return httpx.Response(200, json={"status": status, "document_id": 7, "contract_version": "1"})

    _patch_transport(monkeypatch, handler)
    out = await _push(_pusher())
    assert out.move is expected and out.status == status and out.fatal is False
    if expected is MoveAction.PROCESSED:
        assert out.document_id == 7


@pytest.mark.asyncio
async def test_metadata_shape(monkeypatch):
    captured = {}

    def handler(request):
        # the metadata form field is the multipart body; parse it out loosely
        body = request.content.decode("latin-1")
        start = body.index("{")
        captured["meta"] = json.loads(body[start:body.index("}") + 1])
        return httpx.Response(200, json={"status": "ingested", "contract_version": "1"})

    _patch_transport(monkeypatch, handler)
    await _push(_pusher())
    m = captured["meta"]
    assert m["filename"] == "a.pdf" and m["root"] == "r" and m["sha256"] == "deadbeef"


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [401, 403])
async def test_401_403_is_fatal(monkeypatch, code):
    _patch_transport(monkeypatch, lambda req: httpx.Response(code, text="no"))
    out = await _push(_pusher())
    assert out.fatal is True and out.move is MoveAction.LEAVE


@pytest.mark.asyncio
async def test_503_is_leave(monkeypatch):
    _patch_transport(monkeypatch, lambda req: httpx.Response(503, json={"detail": {"reason": "worker_unavailable"}}))
    out = await _push(_pusher())
    assert out.move is MoveAction.LEAVE and out.fatal is False


@pytest.mark.asyncio
async def test_network_error_is_leave(monkeypatch):
    def handler(request):
        raise httpx.ConnectError("down")

    _patch_transport(monkeypatch, handler)
    out = await _push(_pusher())
    assert out.move is MoveAction.LEAVE and "transport_error" in (out.detail or "")


@pytest.mark.asyncio
async def test_200_unparseable_body_is_leave(monkeypatch):
    _patch_transport(monkeypatch, lambda req: httpx.Response(200, text="not json"))
    out = await _push(_pusher())
    assert out.move is MoveAction.LEAVE
