from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from messaging.clock import FixedClock
from messaging.config import Settings
from messaging.context import sign
from messaging.directory import StaticDirectory
from messaging.main import build_services
from messaging.ratelimit import Limit, RateLimiter
from messaging.realtime import RecordingPublisher
from messaging.services.common import Actor
from tests.store_factory import make_store

KEY = b"k" * 32
KEY_B64 = base64.b64encode(KEY).decode()
T0 = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)

PRIYA, AHMED, LINA, MARCUS, FORMER, OTHER = 101, 102, 103, 104, 105, 201
DEV_DIRECTORY = json.loads((Path(__file__).parents[1] / "dev" / "directory.dev.json").read_text(encoding="utf-8"))


def make_settings(**overrides) -> Settings:
    values = dict(environment="test", dev_mode=True, store="memory", directory="static",
                  context_signing_keys=KEY_B64, segment_size=5)
    values.update(overrides)
    return Settings(_env_file=None, **values)


class Harness:
    def __init__(self, directory_data=None, limits=None):
        self.clock = FixedClock(T0)
        self.realtime = RecordingPublisher()
        self.store = make_store(segment_size=5)   # MSG_TEST_STORE=azure replays everything on Azurite
        self.directory = StaticDirectory(directory_data or DEV_DIRECTORY)
        self.settings = make_settings()
        # Generous limits by default so tests exercise rules, not throttles.
        unlimited = {k: Limit(capacity=10_000, per_second=10_000) for k in (
            "message.send", "thread.create", "thread.members", "typing", "task.create",
            "task.update", "task.request_update", "people.search")}
        self.services = build_services(self.settings, store=self.store, directory=self.directory,
                                       realtime=self.realtime, clock=self.clock,
                                       limiter=RateLimiter(limits or unlimited))
        self.threads = self.services.threads
        self.tasks = self.services.tasks

    async def actor(self, user_id: int) -> Actor:
        users = {u["userId"]: u for u in DEV_DIRECTORY["users"]}
        user = await self.directory.resolve_email(users[user_id]["email"])
        return Actor(user=user)

    def context(self, actor: Actor, *, kind="insight", resolution="report", report_row_id=11,
                title="ST2 sales 18% under target", key="insight:target_tracker:4:2026-09-15",
                snapshot=None, iat=None, ttl=300, **anchor_extra) -> str:
        now = int(self.clock.now().timestamp()) if iat is None else iat
        anchor = {"kind": kind, "resolution": resolution, "key": key, "title": title,
                  "reportRowId": report_row_id, **anchor_extra}
        return sign({"v": 1, "customerId": actor.customer_id, "userId": actor.user_id, "iat": now,
                     "exp": now + ttl, "anchor": anchor,
                     "snapshot": snapshot or {"metric": "Net sales", "value": "USD 55.3K", "delta": "-18%"}}, KEY)

    @staticmethod
    def ulid() -> str:
        from messaging.ids import new_ulid
        return new_ulid()


@pytest.fixture
async def h():
    harness = Harness()
    yield harness
    await harness.store.close()
