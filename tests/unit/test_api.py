from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from messaging.config import Settings
from messaging.ids import new_ulid
from messaging.main import create_app
from tests.conftest import AHMED, Harness, KEY_B64, PRIYA

pytestmark = pytest.mark.unit

PRIYA_EMAIL, AHMED_EMAIL, OTHER_EMAIL = "priya@sample.test", "ahmed@sample.test", "other@othercustomer.test"


@pytest.fixture
def api():
    h = Harness()
    app = create_app(h.settings, services=h.services)
    with TestClient(app) as client:
        client.h = h
        yield client


def as_user(email):
    return {"X-Dev-User": email}


def test_requires_identity(api):
    r = api.get("/v1/me")
    assert r.status_code == 401 and r.json()["code"] == "unauthenticated"


def test_unknown_person_is_refused(api):
    r = api.get("/v1/me", headers=as_user("stranger@nowhere.test"))
    assert r.status_code == 403 and r.json()["code"] == "messaging_not_enabled"


def test_me_has_no_email_and_carries_security_headers(api):
    r = api.get("/v1/me", headers=as_user(PRIYA_EMAIL))
    assert r.status_code == 200
    assert "email" not in r.text.lower().replace("messaging", "")
    assert r.json()["permissions"]["teamTasks"] is True
    assert r.headers["X-Content-Type-Options"] == "nosniff"
    assert r.headers["Cache-Control"] == "no-store"


def test_conversation_round_trip(api):
    started = api.post("/v1/threads", headers=as_user(PRIYA_EMAIL), json={
        "type": "direct", "memberIds": [AHMED], "message": {"id": new_ulid(), "text": "Morning - ST2 numbers?"}})
    assert started.status_code == 201, started.text
    thread_id = started.json()["id"]

    inbox = api.get("/v1/threads", headers=as_user(AHMED_EMAIL)).json()
    assert inbox["items"][0]["unread"] is True
    assert inbox["people"][str(PRIYA)]["name"] == "Priya Nair"

    reply = api.post(f"/v1/threads/{thread_id}/messages", headers=as_user(AHMED_EMAIL),
                     json={"id": new_ulid(), "text": "Short staffed Tuesday."})
    assert reply.status_code == 201
    messages = api.get(f"/v1/threads/{thread_id}/messages", headers=as_user(PRIYA_EMAIL)).json()["messages"]
    assert [m["text"] for m in messages] == ["Morning - ST2 numbers?", "Short staffed Tuesday."]

    assert api.get(f"/v1/threads/{thread_id}", headers=as_user(OTHER_EMAIL)).status_code == 404


def test_task_round_trip_and_activity(api):
    created = api.post("/v1/tasks", headers={**as_user(PRIYA_EMAIL), "Idempotency-Key": "api-task-0001"},
                       json={"title": "Fix ST2 staffing gap", "ownerId": AHMED, "needsApproval": True})
    assert created.status_code == 201, created.text
    task = created.json()

    activity = api.get("/v1/activity", headers=as_user(AHMED_EMAIL)).json()
    assert activity["unreadNotifications"] == 1

    moved = api.post(f"/v1/tasks/{task['id']}/status", headers=as_user(AHMED_EMAIL),
                     json={"status": "done", "expectedVersion": task["version"]})
    assert moved.status_code == 200 and moved.json()["awaitingApproval"] is True

    stale = api.post(f"/v1/tasks/{task['id']}/status", headers=as_user(PRIYA_EMAIL),
                     json={"status": "closed", "expectedVersion": task["version"]})
    assert stale.status_code == 409 and stale.json()["code"] == "version_conflict"


def test_validation_errors_are_flat_and_do_not_echo_input(api):
    r = api.post("/v1/threads", headers=as_user(PRIYA_EMAIL), json={"type": "direct", "memberIds": "nope", "extra": 1})
    body = r.json()
    assert r.status_code == 422 and body["code"] == "invalid_request"
    assert "memberIds" in body["fields"] and "extra" in body["fields"]
    assert "nope" not in r.text


def test_oversized_body_is_refused_before_parsing(api):
    r = api.post("/v1/threads", headers={**as_user(PRIYA_EMAIL), "Content-Type": "application/json",
                                         "Content-Length": "999999"}, content=b"{}")
    assert r.status_code == 413


def test_rate_limit_has_retry_after(api):
    from messaging.ratelimit import Limit, RateLimiter
    api.h.services.deps.limiter = RateLimiter({"people.search": Limit(capacity=1, per_second=0.001)})
    api.post("/v1/people/search", headers=as_user(PRIYA_EMAIL), json={"query": "a"})
    r = api.post("/v1/people/search", headers=as_user(PRIYA_EMAIL), json={"query": "a"})
    assert r.status_code == 429 and int(r.headers["Retry-After"]) >= 1


def test_people_search_is_scoped_to_customer_and_flags_ineligible(api):
    h = api.h
    import asyncio
    priya = asyncio.run(h.actor(PRIYA))
    r = api.post("/v1/people/search", headers=as_user(PRIYA_EMAIL), json={"query": "", "context": h.context(priya)})
    people = {p["name"]: p for p in r.json()["people"]}
    assert "Other Customer User" not in people and "Sam Former" not in people and "Priya Nair" not in people
    assert people["Ahmed Rahman"]["eligible"] is True
    assert people["Lina Fernandes"] == {**people["Lina Fernandes"], "eligible": False, "reason": "no_report_access"}


def test_docs_hidden_in_production_settings_guard():
    with pytest.raises(ValueError) as err:
        Settings(_env_file=None, environment="production", dev_mode=True, store="memory",
                 directory="static", context_signing_keys=KEY_B64)
    message = str(err.value)
    for expected in ("MSG_DEV_MODE", "MSG_STORE", "MSG_DIRECTORY"):
        assert expected in message


def test_signing_key_must_be_long_enough():
    with pytest.raises(ValueError):
        Settings(_env_file=None, dev_mode=True, context_signing_keys="c2hvcnQ=")
