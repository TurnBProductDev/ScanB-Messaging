from __future__ import annotations

from datetime import timedelta

import pytest

from tests.conftest import AHMED, PRIYA

pytestmark = pytest.mark.unit


async def kinds_for(h, user_id):
    actor = await h.actor(user_id)
    return [n["kind"] for n in (await h.threads.notifications(actor))["items"] if n["kind"].startswith(("reminder", "escalation"))]


async def test_due_soon_is_sent_once_per_due_date(h):
    priya = await h.actor(PRIYA)
    task = await h.tasks.create(priya, title="Sign off range plan", owner_id=AHMED,
                                due_at=h.clock.now() + timedelta(hours=30))
    await h.services.reminders.run_once()
    assert await kinds_for(h, AHMED) == []                     # not yet within 24h

    h.clock.advance(hours=8)
    await h.services.reminders.run_once()
    await h.services.reminders.run_once()                      # a second pass, or a second replica
    assert await kinds_for(h, AHMED) == ["reminder_due_soon"]

    await h.tasks.update(priya, task["id"], changes={"dueAt": (h.clock.now() + timedelta(hours=10)).isoformat()},
                         expected_version=task["version"])
    await h.services.reminders.run_once()
    assert (await kinds_for(h, AHMED)).count("reminder_due_soon") == 2   # a new due date earns a new reminder


async def test_overdue_notifies_owner_daily_and_assigner_on_the_first_day(h):
    priya = await h.actor(PRIYA)
    await h.tasks.create(priya, title="Clear place-order lines", owner_id=AHMED,
                         due_at=h.clock.now() + timedelta(hours=1))
    h.clock.advance(hours=2)
    await h.services.reminders.run_once()
    await h.services.reminders.run_once()
    assert (await kinds_for(h, AHMED)).count("reminder_overdue") == 1
    assert (await kinds_for(h, PRIYA)).count("reminder_overdue") == 1

    h.clock.advance(days=1)
    await h.services.reminders.run_once()
    assert (await kinds_for(h, AHMED)).count("reminder_overdue") == 2
    assert (await kinds_for(h, PRIYA)).count("reminder_overdue") == 1


async def test_blocked_too_long_escalates_to_assigner_once(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await h.tasks.create(priya, title="Fix staffing", owner_id=AHMED)
    await h.tasks.change_status(ahmed, task["id"], to="blocked", note="Waiting on HR", expected_version=1)
    h.clock.advance(days=3)
    await h.services.reminders.run_once()
    await h.services.reminders.run_once()
    assert (await kinds_for(h, PRIYA)).count("escalation_blocked") == 1


async def test_no_update_nudges_owner_once_per_quiet_spell(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await h.tasks.create(priya, title="Check oils promo", owner_id=AHMED)
    h.clock.advance(days=4)
    await h.services.reminders.run_once()
    await h.services.reminders.run_once()
    assert (await kinds_for(h, AHMED)).count("reminder_no_update") == 1

    task = await h.tasks.change_status(ahmed, task["id"], to="in_progress", note=None, expected_version=1)
    h.clock.advance(days=4)
    await h.services.reminders.run_once()
    assert (await kinds_for(h, AHMED)).count("reminder_no_update") == 2


async def test_waiting_for_approval_reminds_assigner(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await h.tasks.create(priya, title="Range plan", owner_id=AHMED, needs_approval=True)
    await h.tasks.change_status(ahmed, task["id"], to="done", note=None, expected_version=1)
    h.clock.advance(days=3)
    await h.services.reminders.run_once()
    assert (await kinds_for(h, PRIYA)).count("reminder_approval") == 1


async def test_reminders_do_not_bump_task_version(h):
    priya = await h.actor(PRIYA)
    task = await h.tasks.create(priya, title="Anything", owner_id=AHMED, due_at=h.clock.now() + timedelta(hours=2))
    await h.services.reminders.run_once()
    assert (await h.tasks.get(priya, task["id"]))["version"] == task["version"]


async def test_finished_tasks_are_left_alone(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    task = await h.tasks.create(priya, title="Done already", owner_id=AHMED, due_at=h.clock.now() + timedelta(hours=1))
    await h.tasks.change_status(ahmed, task["id"], to="done", note=None, expected_version=1)
    h.clock.advance(days=5)
    stats = await h.services.reminders.run_once()
    assert stats.sent == {}
