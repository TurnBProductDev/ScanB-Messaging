from __future__ import annotations

from datetime import timedelta

import pytest

from messaging.errors import Conflict, Forbidden, Invalid, NotFound, RateLimited
from messaging.ratelimit import Limit
from tests.conftest import AHMED, Harness, LINA, MARCUS, OTHER, PRIYA

pytestmark = pytest.mark.unit


async def make_task(h, creator, owner=AHMED, **kw):
    kw.setdefault("title", "Fix ST2 staffing gap")
    return await h.tasks.create(creator, owner_id=owner, **kw)


async def move(h, actor, task, to, note=None):
    return await h.tasks.change_status(actor, task["id"], to=to, note=note, expected_version=task["version"])


# ─────────────────────────────────────────────────────────── create ──
async def test_task_lands_in_each_persons_list_and_notifies_owner(h):
    priya, ahmed, marcus = await h.actor(PRIYA), await h.actor(AHMED), await h.actor(MARCUS)
    task = await make_task(h, priya, watcher_ids=[MARCUS], due_at=h.clock.now() + timedelta(days=2))

    assert [t["id"] for t in (await h.tasks.list(ahmed, view="assigned_to_me"))["items"]] == [task["id"]]
    assert [t["id"] for t in (await h.tasks.list(priya, view="assigned_by_me"))["items"]] == [task["id"]]
    assert [t["id"] for t in (await h.tasks.list(marcus, view="watching"))["items"]] == [task["id"]]
    kinds = [n["kind"] for n in (await h.threads.notifications(ahmed))["items"]]
    assert kinds == ["task_assigned"]
    assert (await h.threads.get_thread(ahmed, task["threadId"]))["task"]["id"] == task["id"]


async def test_task_from_insight_requires_owner_access(h):
    priya = await h.actor(PRIYA)
    with pytest.raises(Invalid) as err:
        await make_task(h, priya, owner=LINA, context_token=h.context(priya))   # Lina lacks report 11
    assert err.value.code == "ineligible_people"


async def test_cannot_assign_outside_customer(h):
    priya = await h.actor(PRIYA)
    with pytest.raises(Invalid):
        await make_task(h, priya, owner=OTHER)


async def test_task_created_from_conversation_inherits_item_and_leaves_note(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="context", member_ids=[AHMED], context_token=h.context(priya))
    msg = await h.threads.post_message(ahmed, thread["id"], message_id=h.ulid(), text="Short staffed Tuesday")
    task = await make_task(h, priya, source_thread_id=thread["id"], source_message_id=msg["id"])

    assert task["context"]["visible"] is True and task["context"]["snapshot"]["value"] == "USD 55.3K"
    last = (await h.threads.list_messages(ahmed, thread["id"]))["messages"][-1]
    assert last["kind"] == "system" and last["system"]["taskId"] == task["id"]


async def test_due_date_in_the_past_is_rejected(h):
    priya = await h.actor(PRIYA)
    with pytest.raises(Invalid):
        await make_task(h, priya, due_at=h.clock.now() - timedelta(days=3))


async def test_idempotent_task_creation(h):
    priya = await h.actor(PRIYA)
    a = await make_task(h, priya, idempotency_key="task-key-0001")
    b = await make_task(h, priya, idempotency_key="task-key-0001")
    assert a["id"] == b["id"]
    assert len((await h.tasks.list(priya, view="assigned_by_me"))["items"]) == 1


# ─────────────────────────────────────────────────────────── status ──
async def test_owner_works_the_task_and_creator_approves(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await make_task(h, priya, needs_approval=True)

    task = await move(h, ahmed, task, "in_progress")
    task = await move(h, ahmed, task, "done")
    assert task["awaitingApproval"] is True
    assert any(n["kind"] == "task_awaiting_approval" for n in (await h.threads.notifications(priya))["items"])

    with pytest.raises(Forbidden):
        await move(h, ahmed, task, "closed")                  # owners cannot approve their own work
    task = await move(h, priya, task, "closed")
    assert task["status"] == "closed" and task["completedAt"] is not None
    assert [e["event"] for e in task["history"]] == ["created", "status_changed", "status_changed", "status_changed"]


async def test_blocking_and_reopening_need_a_note(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await make_task(h, priya)
    with pytest.raises(Invalid) as err:
        await move(h, ahmed, task, "blocked")
    assert err.value.code == "note_required"
    task = await move(h, ahmed, task, "blocked", note="Waiting on rota from HR")
    task = await move(h, ahmed, task, "done")
    with pytest.raises(Invalid):
        await move(h, priya, task, "in_progress")             # creator reopening
    task = await move(h, priya, task, "in_progress", note="Still short on Friday")
    assert task["status"] == "in_progress" and task["completedAt"] is None


async def test_owner_can_take_back_done_without_note(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await make_task(h, priya)
    task = await move(h, ahmed, task, "done")
    task = await move(h, ahmed, task, "in_progress")
    assert task["status"] == "in_progress"


async def test_watchers_cannot_change_status_and_strangers_cannot_see(h):
    priya, marcus, lina = await h.actor(PRIYA), await h.actor(MARCUS), await h.actor(LINA)
    task = await make_task(h, priya, watcher_ids=[MARCUS])
    with pytest.raises(Forbidden):
        await move(h, marcus, task, "in_progress")
    with pytest.raises(NotFound):
        await h.tasks.get(lina, task["id"])


async def test_stale_version_is_a_conflict(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await make_task(h, priya)
    await move(h, ahmed, task, "in_progress")
    with pytest.raises(Conflict) as err:
        await move(h, ahmed, task, "blocked", note="stale")    # still holding version 1
    assert err.value.extra["currentVersion"] == 2


async def test_creator_cancels_and_restores(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await make_task(h, priya)
    with pytest.raises(Forbidden):
        await move(h, ahmed, task, "cancelled", note="not mine")
    task = await move(h, priya, task, "cancelled", note="Covered by another plan")
    assert any(n["kind"] == "task_cancelled" for n in (await h.threads.notifications(ahmed))["items"])
    task = await move(h, priya, task, "todo")
    assert task["status"] == "todo"


# ─────────────────────────────────────────────────────────── edits ──
async def test_reassignment_moves_access_and_conversation(h):
    priya, ahmed, marcus = await h.actor(PRIYA), await h.actor(AHMED), await h.actor(MARCUS)
    task = await make_task(h, priya)
    task = await h.tasks.update(priya, task["id"], changes={"ownerId": MARCUS}, expected_version=task["version"])

    assert task["ownerId"] == MARCUS
    with pytest.raises(NotFound):
        await h.tasks.get(ahmed, task["id"])
    with pytest.raises(NotFound):
        await h.threads.get_thread(ahmed, task["threadId"])
    assert (await h.tasks.list(ahmed, view="assigned_to_me"))["items"] == []
    assert [t["id"] for t in (await h.tasks.list(marcus, view="assigned_to_me"))["items"]] == [task["id"]]
    assert any(n["kind"] == "task_reassigned" for n in (await h.threads.notifications(ahmed))["items"])


async def test_only_creator_edits(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await make_task(h, priya)
    with pytest.raises(Forbidden):
        await h.tasks.update(ahmed, task["id"], changes={"title": "mine now"}, expected_version=task["version"])
    with pytest.raises(Invalid):
        await h.tasks.update(priya, task["id"], changes={"status": "closed"}, expected_version=task["version"])


async def test_reassigning_to_someone_without_item_access_is_refused(h):
    priya = await h.actor(PRIYA)
    task = await make_task(h, priya, context_token=h.context(priya))
    with pytest.raises(Invalid):
        await h.tasks.update(priya, task["id"], changes={"ownerId": LINA}, expected_version=task["version"])


# ─────────────────────────────────────────────────── request update ──
async def test_request_update(h):
    priya, ahmed, marcus = await h.actor(PRIYA), await h.actor(AHMED), await h.actor(MARCUS)
    task = await make_task(h, priya, watcher_ids=[MARCUS])
    with pytest.raises(Forbidden):
        await h.tasks.request_update(ahmed, task["id"], note=None)
    await h.tasks.request_update(marcus, task["id"], note="Any progress?")
    notes = (await h.threads.notifications(ahmed))["items"]
    assert notes[0]["kind"] == "task_update_requested" and notes[0]["body"] == "Any progress?"


async def test_request_update_is_rate_limited_per_task():
    h = Harness(limits={"task.request_update": Limit(capacity=1, per_second=0.0001),
                        "task.create": Limit(capacity=100, per_second=100),
                        "message.send": Limit(capacity=100, per_second=100)})
    priya = await h.actor(PRIYA)
    first = await make_task(h, priya)
    second = await make_task(h, priya, title="Another")
    await h.tasks.request_update(priya, first["id"], note=None)
    await h.tasks.request_update(priya, second["id"], note=None)     # a different task is a different nudge
    with pytest.raises(RateLimited):
        await h.tasks.request_update(priya, first["id"], note=None)


# ─────────────────────────────────────────────────── views and counts ──
async def test_team_view_needs_permission_and_does_not_open_conversations(h):
    priya, ahmed, lina = await h.actor(PRIYA), await h.actor(AHMED), await h.actor(LINA)
    task = await make_task(h, ahmed, owner=LINA)             # Priya is not involved

    with pytest.raises(Forbidden):
        await h.tasks.list(ahmed, view="team")
    team = await h.tasks.list(priya, view="team")            # Priya holds canViewTeamTasks
    assert [t["id"] for t in team["items"]] == [task["id"]]
    assert team["items"][0]["threadId"] is None
    seen = await h.tasks.get(priya, task["id"])
    assert seen["permissions"] == {"transitions": [], "edit": False, "requestUpdate": True}
    with pytest.raises(NotFound):
        await h.threads.get_thread(priya, task["threadId"])


async def test_summary_and_filters(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    now = h.clock.now()
    overdue = await make_task(h, priya, title="Overdue one", due_at=now + timedelta(hours=1))
    await make_task(h, priya, title="This week", due_at=now + timedelta(days=3), priority="high")
    blocked = await make_task(h, priya, title="Blocked one")
    await move(h, ahmed, blocked, "blocked", note="Waiting")
    h.clock.advance(hours=2)

    result = await h.tasks.list(ahmed, view="assigned_to_me")
    assert result["summary"] == {"overdue": 1, "dueThisWeek": 1, "blocked": 1, "awaitingApproval": 0, "doneThisWeek": 0}
    assert result["items"][0]["id"] == overdue["id"]           # overdue sorts first
    assert [t["title"] for t in (await h.tasks.list(ahmed, view="assigned_to_me", due="overdue"))["items"]] == ["Overdue one"]
    assert [t["title"] for t in (await h.tasks.list(ahmed, view="assigned_to_me", priority="high"))["items"]] == ["This week"]
    assert [t["title"] for t in (await h.tasks.list(ahmed, view="assigned_to_me", status="blocked"))["items"]] == ["Blocked one"]


async def test_open_task_counts_by_item_only_count_what_the_viewer_can_see(h):
    priya, ahmed, marcus = await h.actor(PRIYA), await h.actor(AHMED), await h.actor(MARCUS)
    key = "insight:target_tracker:4:2026-09-15"
    await make_task(h, priya, context_token=h.context(priya))
    assert (await h.tasks.counts_by_anchor(ahmed, [key]))["counts"][key] == 1
    assert (await h.tasks.counts_by_anchor(marcus, [key]))["counts"][key] == 0


async def test_lost_item_access_hides_task_context(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await make_task(h, priya, context_token=h.context(priya))
    h.directory._grants[AHMED] = set()
    view = await h.tasks.get(ahmed, task["id"])
    assert view["context"]["visible"] is False and "snapshot" not in view["context"]


async def test_malformed_source_conversation_is_not_found(h):
    priya = await h.actor(PRIYA)
    with pytest.raises(NotFound):
        await make_task(h, priya, source_thread_id="a/b#c")
