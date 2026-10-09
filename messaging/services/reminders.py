"""Scheduled checks on task status: reminders and escalations.

Runs every few minutes (messaging.worker). Safe to run on several replicas at
once: a reminder is only sent by the run whose write of the reminder marker
succeeds, and the marker is re-checked inside that write. Two runs racing on
the same task cannot both notify.

Reminder markers are bookkeeping, not a change to the task, so they do not
bump the task's version; a person holding the task open is not told it changed
underneath them because a reminder went out.

    due soon     owner, once per due date, within `reminder_due_soon_hours`
    overdue      owner every day it stays overdue; the assigner on the first day
    no update    owner, once per quiet spell of `reminder_stale_days`
    blocked      assigner, once, after `escalate_blocked_days`
    approval     assigner, once, when "done" has waited `escalate_blocked_days`
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Optional

from ..ids import new_ulid
from ..models import Task, TaskEvent, TaskStatus
from ..storage.base import TASK_INDEX_ALL
from .common import Auditor, Deps, Notifier

logger = logging.getLogger("messaging.reminders")


@dataclass
class RunStats:
    tenants: int = 0
    tasks_checked: int = 0
    sent: dict[str, int] = field(default_factory=dict)

    def count(self, kind: str) -> None:
        self.sent[kind] = self.sent.get(kind, 0) + 1


class ReminderService:
    def __init__(self, deps: Deps, notifier: Notifier, auditor: Auditor) -> None:
        self.d = deps
        self.notifier = notifier
        self.audit = auditor

    async def run_once(self) -> RunStats:
        stats = RunStats()
        for customer_id in await self.d.store.list_tenants():
            stats.tenants += 1
            try:
                await self.run_tenant(customer_id, stats)
            except Exception:
                logger.exception("reminder run failed for customer %s", customer_id)
        return stats

    async def run_tenant(self, customer_id: int, stats: Optional[RunStats] = None) -> RunStats:
        stats = stats or RunStats()
        s = self.d.settings
        for indexed in await self.d.store.list_task_index(customer_id, TASK_INDEX_ALL):
            if not (indexed.is_open() or indexed.awaiting_approval()):
                continue
            stats.tasks_checked += 1
            now = self.d.clock.now()

            due_soon = timedelta(hours=s.reminder_due_soon_hours)
            await self._maybe(customer_id, indexed.id, "reminder_due_soon", stats,
                              when=lambda t: t.is_open() and t.dueAt is not None and now < t.dueAt <= now + due_soon
                              and t.reminders.dueSoonFor != t.dueAt,
                              mark=lambda t: setattr(t.reminders, "dueSoonFor", t.dueAt),
                              recipients=lambda t: [t.ownerId],
                              title=lambda t: "A task you own is due soon")

            today = now.date()
            await self._maybe(customer_id, indexed.id, "reminder_overdue", stats,
                              when=lambda t: t.is_overdue(now) and t.reminders.overdueNoticeOn != today,
                              mark=lambda t: setattr(t.reminders, "overdueNoticeOn", today),
                              recipients=lambda t: [t.ownerId] if t.reminders.overdueNoticeOn else [t.ownerId, t.creatorId],
                              title=lambda t: "A task is overdue")

            stale = timedelta(days=s.reminder_stale_days)
            await self._maybe(customer_id, indexed.id, "reminder_no_update", stats,
                              when=lambda t: t.status in (TaskStatus.TODO, TaskStatus.IN_PROGRESS)
                              and t.lastActivityAt <= now - stale
                              and (t.reminders.staleNoticeAt is None or t.reminders.staleNoticeAt < t.lastActivityAt),
                              mark=lambda t: setattr(t.reminders, "staleNoticeAt", now),
                              recipients=lambda t: [t.ownerId],
                              title=lambda t: f"No update on a task for {s.reminder_stale_days} days")

            escalate = timedelta(days=s.escalate_blocked_days)
            await self._maybe(customer_id, indexed.id, "escalation_blocked", stats,
                              when=lambda t: t.status == TaskStatus.BLOCKED and t.statusChangedAt <= now - escalate
                              and t.reminders.blockedEscalatedAt is None,
                              mark=lambda t: setattr(t.reminders, "blockedEscalatedAt", now),
                              recipients=lambda t: [t.creatorId],
                              title=lambda t: f"A task has been blocked for {s.escalate_blocked_days} days")

            await self._maybe(customer_id, indexed.id, "reminder_approval", stats,
                              when=lambda t: t.awaiting_approval() and t.statusChangedAt <= now - escalate
                              and t.reminders.approvalReminderAt is None,
                              mark=lambda t: setattr(t.reminders, "approvalReminderAt", now),
                              recipients=lambda t: [t.creatorId],
                              title=lambda t: "A task is waiting for your approval")
        return stats

    async def _maybe(self, customer_id: int, task_id: str, kind: str, stats: RunStats, *,
                     when: Callable[[Task], bool], mark: Callable[[Task], None],
                     recipients: Callable[[Task], list[int]], title: Callable[[Task], str]) -> None:
        decided: dict[str, object] = {}

        def mutate(t: Task) -> Task:
            # Re-evaluated on every retry against the freshest copy.
            decided.clear()
            if when(t):
                decided["to"] = recipients(t)     # before marking: overdue reads the old marker
                decided["title"] = title(t)
                mark(t)
            return t

        try:
            updated, _ = await self.d.store.update_task(customer_id, task_id, mutate)
        except KeyError:
            return
        except Exception:
            logger.exception("reminder %s failed for task %s", kind, task_id)
            return
        if not decided:
            return
        # The reminder is recorded on the task before it is sent. If sending
        # fails the person misses one reminder; the alternative, sending first,
        # risks sending it on every run.
        await self.d.store.append_task_event(customer_id, TaskEvent(
            id=new_ulid(), taskId=task_id, at=self.d.clock.now(), actorId=None,
            event="escalated" if kind.startswith("escalation") else "reminder", changes={"kind": kind}))
        await self.notifier.notify(customer_id, decided["to"], kind=kind, title=decided["title"],
                                   body=updated.title, task_id=task_id, thread_id=updated.threadId)
        await self.audit.record(None, customer_id, f"task.{kind}", task_id=task_id, targets=decided["to"])
        stats.count(kind)
