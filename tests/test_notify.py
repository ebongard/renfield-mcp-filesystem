import httpx
import pytest

import renfield_mcp_filesystem.notify as notify_mod
from renfield_mcp_filesystem.notify import LogNotifier, WebhookNotifier, make_notifier


def test_make_notifier_selects_impl():
    assert isinstance(make_notifier(None), LogNotifier)
    assert isinstance(make_notifier("http://hook"), WebhookNotifier)


@pytest.mark.asyncio
async def test_log_notifier_does_not_raise():
    n = LogNotifier()
    await n.failure("r", "a.pdf", "extension_not_allowed")
    await n.fatal("r", "http_403")
    await n.disconnect("r", "connection reset")


@pytest.mark.asyncio
async def test_webhook_notifier_posts(monkeypatch):
    captured = {}

    def handler(request):
        captured["body"] = request.content.decode()
        captured["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"ok": True})

    real = httpx.AsyncClient
    monkeypatch.setattr(
        notify_mod.httpx, "AsyncClient",
        lambda *a, **k: real(transport=httpx.MockTransport(handler), timeout=5),
    )

    n = WebhookNotifier("http://hook", token="tok")
    await n.failure("docs", "a.pdf", "oversize")
    assert "renfield-mcp-filesystem" in captured["body"]
    assert "oversize" in captured["body"] and "docs" in captured["body"]
    assert captured["auth"] == "Bearer tok"


@pytest.mark.asyncio
async def test_webhook_failure_is_swallowed(monkeypatch):
    def handler(request):
        raise httpx.ConnectError("hook down")

    real = httpx.AsyncClient
    monkeypatch.setattr(
        notify_mod.httpx, "AsyncClient",
        lambda *a, **k: real(transport=httpx.MockTransport(handler), timeout=5),
    )
    n = WebhookNotifier("http://hook")
    # must not raise — operator notify can't break the ingest path
    await n.disconnect("docs", "reset")
