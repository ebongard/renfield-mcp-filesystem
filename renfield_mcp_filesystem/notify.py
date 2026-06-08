"""Operator notification (T17) — never quieter on failure than on success.

A file landing in ``failed/``, a retry-backlog give-up, a fatal token error, or a
share disconnect all reach the operator. The default :class:`LogNotifier` emits
loud structured ERROR logs (what container log alerting watches); a
:class:`WebhookNotifier` additionally POSTs to a configured webhook (e.g.
Renfield's notification webhook) when ``FILES_NOTIFY_WEBHOOK_URL`` is set.
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger("renfield-mcp-filesystem.notify")


class Notifier:
    async def failure(self, root: str, relpath: str, reason: str) -> None:
        raise NotImplementedError

    async def fatal(self, root: str, reason: str) -> None:
        raise NotImplementedError

    async def disconnect(self, root: str, reason: str) -> None:
        raise NotImplementedError


class LogNotifier(Notifier):
    async def failure(self, root: str, relpath: str, reason: str) -> None:
        logger.error("OPERATOR-NOTIFY failure root=%s file=%s reason=%s", root, relpath, reason)

    async def fatal(self, root: str, reason: str) -> None:
        logger.error("OPERATOR-NOTIFY fatal root=%s reason=%s", root, reason)

    async def disconnect(self, root: str, reason: str) -> None:
        logger.error("OPERATOR-NOTIFY disconnect root=%s reason=%s", root, reason)


class WebhookNotifier(Notifier):
    """Logs (via an inner LogNotifier) AND best-effort POSTs a compact JSON
    notification to ``webhook_url``. A webhook failure never raises — operator
    notification must not break the ingest path."""

    def __init__(self, webhook_url: str, token: str | None = None, timeout: float = 10.0):
        self._url = webhook_url
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._timeout = timeout
        self._log = LogNotifier()

    async def _post(self, event: str, **fields) -> None:
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                await client.post(
                    self._url, headers=self._headers,
                    json={"source": "renfield-mcp-filesystem", "event": event, **fields},
                )
        except httpx.HTTPError as exc:
            logger.warning("notify webhook POST failed: %s", exc)

    async def failure(self, root: str, relpath: str, reason: str) -> None:
        await self._log.failure(root, relpath, reason)
        await self._post("failure", root=root, relpath=relpath, reason=reason)

    async def fatal(self, root: str, reason: str) -> None:
        await self._log.fatal(root, reason)
        await self._post("fatal", root=root, reason=reason)

    async def disconnect(self, root: str, reason: str) -> None:
        await self._log.disconnect(root, reason)
        await self._post("disconnect", root=root, reason=reason)


def make_notifier(webhook_url: str | None, token: str | None = None) -> Notifier:
    return WebhookNotifier(webhook_url, token) if webhook_url else LogNotifier()
