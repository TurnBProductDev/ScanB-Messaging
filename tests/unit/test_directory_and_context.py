from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
import pytest

from messaging.context import sign, verify
from messaging.directory import CachedDirectory, DirectoryUnavailable, StaticDirectory, WebAppDirectory
from messaging.errors import Invalid
from messaging.models import Anchor
from tests.conftest import AHMED, DEV_DIRECTORY, KEY, LINA, PRIYA

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)
ANCHOR = {"kind": "insight", "resolution": "report", "key": "insight:x:1:2026-09-15", "title": "T", "reportRowId": 11}


def token(**over):
    payload = {"v": 1, "customerId": 1, "userId": PRIYA, "iat": int(NOW.timestamp()),
               "exp": int(NOW.timestamp()) + 300, "anchor": ANCHOR, "snapshot": {"a": 1}}
    payload.update(over)
    return sign(payload, KEY)


def check(tok, keys=(KEY,), **kw):
    args = dict(now=NOW, max_age_seconds=300, customer_id=1, user_id=PRIYA)
    args.update(kw)
    return verify(tok, list(keys), **args)


# ─────────────────────────────────────────────────────────── context ──
def test_valid_context():
    assert check(token()).anchor.reportRowId == 11


def test_rotated_keys_verify():
    old, new = b"o" * 32, b"n" * 32
    assert check(sign(json.loads(json.dumps({"v": 1, "customerId": 1, "userId": PRIYA, "iat": int(NOW.timestamp()),
                                             "exp": int(NOW.timestamp()) + 60, "anchor": ANCHOR, "snapshot": {}})), old),
                 keys=(new, old)).anchor.key == ANCHOR["key"]


@pytest.mark.parametrize("override,reason", [
    ({"customerId": 2}, "audience"),
    ({"userId": AHMED}, "audience"),
    ({"exp": int(NOW.timestamp()) + 3600}, "expiry"),                 # lifetime longer than allowed
    ({"iat": int(NOW.timestamp()) + 600, "exp": int(NOW.timestamp()) + 700}, "expiry"),
    ({"v": 2}, "version"),
    ({"snapshot": {"big": "x" * 40_000}}, "snapshot_too_large"),
    ({"anchor": {**ANCHOR, "url": "https://evil.example/phish"}}, "anchor"),
    ({"anchor": {**ANCHOR, "url": "//evil.example"}}, "anchor"),
    ({"anchor": {**ANCHOR, "key": "a/b"}}, "anchor"),
])
def test_context_rejections(override, reason):
    with pytest.raises(Invalid) as err:
        check(token(**override))
    assert err.value.extra["reason"] == reason


def test_wrong_key_is_rejected():
    with pytest.raises(Invalid) as err:
        check(token(), keys=(b"z" * 32,))
    assert err.value.extra["reason"] == "signature"


def test_app_relative_url_allowed():
    assert Anchor.model_validate({**ANCHOR, "url": "/Modules/Dashboards/3"}).url == "/Modules/Dashboards/3"


# ───────────────────────────────────────────────────────── directory ──
async def test_static_directory_rules():
    d = StaticDirectory(DEV_DIRECTORY)
    module_anchor = Anchor.model_validate({**ANCHOR, "resolution": "module", "reportRowId": None, "moduleId": 3})
    decisions = await d.check_anchor(1, [AHMED, LINA], module_anchor)
    assert decisions[AHMED].allowed and decisions[LINA].allowed        # both hold a report in module 3
    pb_anchor = Anchor.model_validate({**ANCHOR, "reportRowId": None, "pbReportId": "5f2a1b3c-0000-4000-8000-000000000011"})
    decisions = await d.check_anchor(1, [AHMED, LINA], pb_anchor)
    assert decisions[AHMED].allowed and not decisions[LINA].allowed


async def test_cache_serves_within_ttl_and_never_caches_failures():
    calls = {"n": 0, "fail": False}

    class Flaky(StaticDirectory):
        async def resolve_email(self, email):
            calls["n"] += 1
            if calls["fail"]:
                raise DirectoryUnavailable("down")
            return await super().resolve_email(email)

    clock = {"t": 0.0}
    cached = CachedDirectory(Flaky(DEV_DIRECTORY), ttl_seconds=60, clock=lambda: clock["t"])
    await cached.resolve_email("priya@sample.test")
    await cached.resolve_email("priya@sample.test")
    assert calls["n"] == 1
    clock["t"] = 61
    calls["fail"] = True
    with pytest.raises(DirectoryUnavailable):
        await cached.resolve_email("priya@sample.test")
    calls["fail"] = False
    assert (await cached.resolve_email("priya@sample.test")).userId == PRIYA


def _webapp(handler):
    d = WebAppDirectory(base_url="https://webapp.test", dev_key="local-key")
    d._http = httpx.AsyncClient(base_url="https://webapp.test", transport=httpx.MockTransport(handler))
    return d


async def test_webapp_directory_denies_anyone_not_answered_for():
    def handler(request):
        assert request.headers["X-Internal-Dev-Key"] == "local-key"
        return httpx.Response(200, json={"decisions": [{"userId": AHMED, "allowed": True}]})
    d = _webapp(handler)
    decisions = await d.check_anchor(1, [AHMED, LINA], Anchor.model_validate(ANCHOR))
    assert decisions[AHMED].allowed and not decisions[LINA].allowed and decisions[LINA].reason == "no_answer"


async def test_webapp_directory_fails_closed_on_errors():
    d = _webapp(lambda request: httpx.Response(500))
    with pytest.raises(DirectoryUnavailable):
        await d.resolve_email("priya@sample.test")
    d = _webapp(lambda request: httpx.Response(404))
    assert await d.resolve_email("nobody@sample.test") is None


async def test_webapp_directory_drops_users_from_other_customers():
    def handler(request):
        return httpx.Response(200, json={"users": [
            {"userId": 1, "customerId": 1, "name": "Mine"},
            {"userId": 2, "customerId": 99, "name": "Not mine"}]})
    d = _webapp(handler)
    assert set(await d.get_users(1, [1, 2])) == {1}
