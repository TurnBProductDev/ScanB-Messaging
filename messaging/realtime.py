"""Live delivery.

Storage is the truth and a live event is only a hint that something changed.
A dropped event costs nothing: a reconnecting client asks for what it missed,
and a client that cannot hold a connection at all (a proxy that blocks
WebSockets) polls. So publishing never fails a request.

Azure Web PubSub connections are issued per person, tenant-qualified
("c12-u345"), with no roles: a client can receive what the server sends to
its own user id and nothing else. It cannot join groups or send.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Optional

logger = logging.getLogger("messaging.realtime")


def connection_user_id(customer_id: int, user_id: int) -> str:
    return f"c{customer_id}-u{user_id}"


class RealtimePublisher:
    mode = "poll"

    async def publish(self, customer_id: int, user_ids: Iterable[int], event: dict[str, Any]) -> None:
        return None

    async def client_url(self, customer_id: int, user_id: int) -> Optional[str]:
        return None

    async def close(self) -> None:
        return None


class RecordingPublisher(RealtimePublisher):
    """Tests: remembers what would have been sent."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, int, dict[str, Any]]] = []

    async def publish(self, customer_id, user_ids, event):
        for uid in user_ids:
            self.sent.append((customer_id, uid, event))

    def events_for(self, user_id: int, event_type: str | None = None):
        return [e for _, uid, e in self.sent if uid == user_id and (event_type is None or e.get("type") == event_type)]


class WebPubSubPublisher(RealtimePublisher):
    mode = "webpubsub"

    def __init__(self, *, endpoint: str = "", hub: str, connection_string: str = "") -> None:
        from azure.messaging.webpubsubservice.aio import WebPubSubServiceClient

        if connection_string:
            self._client = WebPubSubServiceClient.from_connection_string(connection_string, hub=hub)
            self._credential = None
        else:
            from azure.identity.aio import DefaultAzureCredential
            self._credential = DefaultAzureCredential()
            self._client = WebPubSubServiceClient(endpoint=endpoint, hub=hub, credential=self._credential)

    async def publish(self, customer_id, user_ids, event):
        payload = json.dumps(event, default=str)
        for uid in set(user_ids):
            try:
                await self._client.send_to_user(connection_user_id(customer_id, uid), payload,
                                                 content_type="application/json")
            except Exception as exc:   # a hint, never a failure
                logger.warning("realtime publish failed for c%s-u%s: %s", customer_id, uid, type(exc).__name__)

    async def client_url(self, customer_id, user_id):
        token = await self._client.get_client_access_token(
            user_id=connection_user_id(customer_id, user_id), minutes_to_expire=60, roles=[])
        return token["url"]

    async def close(self):
        await self._client.close()
        if self._credential is not None:
            await self._credential.close()
