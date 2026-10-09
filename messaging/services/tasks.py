"""Tasks: someone must do something, usually by a date.

Every task has its own conversation (a TASK thread) so the discussion stays
with the work, and a history of events so "who changed what, when" never
depends on reading the chat.

Who may do what
---------------
  owner      moves the work along: start, block, mark done, take "done" back
  creator    shapes the task: edit, reassign, cancel, approve or reopen
  watcher    reads, and may ask for an update
  team view  (the canViewTeamTasks permission, set by a customer admin) reads
             every task in the customer and may ask for an update, but is not
             added to task conversations and never sees one it is not in

Statuses
--------
    todo -> in_progress -> done -> closed        (closed only when needsApproval)
      \        |   ^         |
       \       v   |         v
        -> blocked       reopened -> in_progress
    any open status -> cancelled -> todo (restore)

A note is required to block, cancel or reopen: those are the moments someone
else will later ask "why".
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from .. import views
from ..context import verify as verify_context
from ..errors import Conflict, Forbidden, Invalid, NotFound
from ..ids import is_ulid, new_ulid
from ..models import (
    OPEN_STATUSES, Anchor, ContextSnapshot, Member, MemberRole, Task, TaskEvent,
    TaskPriority, TaskStatus, ThreadHead, ThreadType,
)
from ..storage.base import (
    AlreadyExists, TASK_INDEX_ALL, TASK_INDEX_CREATOR, TASK_INDEX_OWNER, TASK_INDEX_WATCHER,
)
from ..text import clean_text
from .common import Access, Actor, Auditor, Deps, Notifier
from .threads import ThreadService, check_idempotency_key

logger = logging.getLogger("messaging.tasks")

MAX_WATCHERS = 20

STATUS_LABEL = {
    TaskStatus.TODO: "To do",
    TaskStatus.IN_PROGRESS: "In progress",
    TaskStatus.BLOCKED: "Blocked",
    TaskStatus.DONE: "Done",
    TaskStatus.CLOSED: "Closed",
    TaskStatus.CANCELLED: "Cancelled",
}

VIEWS = {"assigned_to_me", "assigned_by_me", "watching", "team"}


def allowed_transitions(task: Task, actor_id: int) -> set[TaskStatus]:
    out: set[TaskStatus] = set()
    s = task.status
    if actor_id == task.ownerId:
        if s == TaskStatus.TODO:
            out |= {TaskStatus.IN_PROGRESS, TaskStatus.BLOCKED, TaskStatus.DONE}
        elif s == TaskStatus.IN_PROGRESS:
            out |= {TaskStatus.TODO, TaskStatus.BLOCKED, TaskStatus.DONE}
        elif s == TaskStatus.BLOCKED:
            out |= {TaskStatus.IN_PROGRESS, TaskStatus.DONE}
        elif s == TaskStatus.DONE:
            out.add(TaskStatus.IN_PROGRESS)
    if actor_id == task.creatorId:
        if s in OPEN_STATUSES:
            out.add(TaskStatus.CANCELLED)
        if s == TaskStatus.DONE:
            out.add(TaskStatus.IN_PROGRESS)
            if task.needsApproval:
                out.add(TaskStatus.CLOSED)
        if s == TaskStatus.CLOSED:
            out.add(TaskStatus.IN_PROGRESS)
        if s == TaskStatus.CANCELLED:
            out.add(TaskStatus.TODO)
    out.discard(s)
    return out


def note_required(task: Task, to: TaskStatus, actor_id: int) -> bool:
    if to in (TaskStatus.BLOCKED, TaskStatus.CANCELLED):
        return True
    reopening = task.status in (TaskStatus.DONE, TaskStatus.CLOSED) and to == TaskStatus.IN_PROGRESS
    # The owner taking back their own "done" is a correction, not a reopen.
    return reopening and not (actor_id == task.ownerId and task.status == TaskStatus.DONE)


class TaskService:
    def __init__(self, deps: Deps, access: Access, auditor: Auditor, notifier: Notifier,
                 threads: ThreadService) -> None:
        self.d = deps
        self.access = access
        self.audit = auditor
        self.notifier = notifier
        self.threads = threads

    # ──────────────────────────────────────────────────────────── create ──
    async def create(self, actor: Actor, *, title: str, description: str = "", owner_id: int,
                     watcher_ids: Optional[list[int]] = None, due_at: Optional[datetime] = None,
                     priority: str = "normal", needs_approval: bool = False,
                     context_token: Optional[str] = None, source_thread_id: Optional[str] = None,
                     source_message_id: Optional[str] = None, idempotency_key: Optional[str] = None) -> dict:
        self.d.limiter.check("task.create", actor.customer_id, actor.user_id)
        check_idempotency_key(idempotency_key)
        cid = actor.customer_id
        now = self.d.clock.now()

        clean_title = clean_text(title, max_chars=200, field="title")
        clean_description = clean_text(description, max_chars=4000, field="description", allow_empty=True)
        try:
            prio = TaskPriority(priority)
        except ValueError:
            raise Invalid("Unknown priority.", code="invalid_priority")
        due = self._check_due(due_at, now)
        owner_id = int(owner_id)
        watchers = sorted({int(w) for w in (watcher_ids or [])} - {owner_id, actor.user_id})
        if len(watchers) > MAX_WATCHERS:
            raise Invalid(f"A task can have at most {MAX_WATCHERS} watchers.", code="too_many_watchers")

        await self.d.store.register_tenant(cid)
        task_id = new_ulid()
        if idempotency_key:
            prior = await self.d.store.claim_idempotency(cid, actor.user_id, f"task:{idempotency_key}", task_id)
            if prior is not None:
                if await self.d.store.get_task(cid, prior) is not None:
                    return await self.get(actor, prior)
                task_id = prior   # an earlier attempt claimed the id but never wrote the task

        await self.access.require_colleagues(actor, [owner_id, *watchers])

        # The item the task is about: attached directly, or inherited from the
        # conversation it was created from.
        anchor: Optional[Anchor] = None
        snapshot: Optional[dict[str, Any]] = None
        if context_token:
            verified = verify_context(context_token, self.d.settings.signing_keys(), now=now,
                                      max_age_seconds=self.d.settings.context_max_age_seconds,
                                      customer_id=cid, user_id=actor.user_id)
            anchor, snapshot = verified.anchor, verified.snapshot
        source_head: Optional[ThreadHead] = None
        if source_thread_id:
            source_head, _ = await self.access.require_member(actor, source_thread_id)
            if source_message_id is not None:
                if not is_ulid(source_message_id) or await self.d.store.find_message_seq(
                        cid, source_thread_id, source_message_id) is None:
                    raise Invalid("The message this task was created from does not exist.", code="invalid_source")
            if anchor is None and source_head.anchor is not None:
                ctx = await self.d.store.get_context(cid, source_thread_id)
                anchor, snapshot = source_head.anchor, (ctx.snapshot if ctx else {})
        elif source_message_id:
            raise Invalid("A source message needs its conversation.", code="invalid_source")

        participants = sorted({actor.user_id, owner_id, *watchers})
        if anchor is not None:
            await self.access.require_anchor_access(cid, participants, anchor, actor_id=actor.user_id)

        thread_id = new_ulid()

        # Order: context -> members -> thread head -> task -> index. A crash
        # before the task leaves an orphan conversation nobody lists; a crash
        # after it leaves a task the idempotent retry finds and completes.
        if anchor is not None:
            try:
                await self.d.store.put_context(cid, thread_id, ContextSnapshot(
                    anchor=anchor, snapshot=snapshot or {}, capturedAt=now, capturedBy=actor.user_id))
            except AlreadyExists:
                pass
        for uid in participants:
            await self.d.store.put_member(cid, Member(
                threadId=thread_id, userId=uid, joinedAt=now, addedBy=actor.user_id,
                role=MemberRole.OWNER if uid == actor.user_id else MemberRole.MEMBER))
        await self.d.store.create_thread(ThreadHead(
            id=thread_id, customerId=cid, type=ThreadType.TASK, title=clean_title, createdBy=actor.user_id,
            createdAt=now, lastMessageAt=now, memberCount=len(participants), anchor=anchor, taskId=task_id))

        task = await self.d.store.create_task(Task(
            id=task_id, customerId=cid, threadId=thread_id, title=clean_title, description=clean_description,
            ownerId=owner_id, creatorId=actor.user_id, watcherIds=watchers, priority=prio, dueAt=due,
            needsApproval=bool(needs_approval), anchor=anchor, sourceThreadId=source_thread_id,
            sourceMessageId=source_message_id, createdAt=now, updatedAt=now, statusChangedAt=now,
            lastActivityAt=now))
        await self.d.store.index_task(task)
        await self._event(task, actor.user_id, "created")

        await self.threads.post_system(cid, thread_id, event="task_created",
                                       text=f"{actor.user.name} created this task",
                                       data={"taskId": task_id, "by": actor.user_id})
        if source_head is not None:
            await self.threads.post_system(cid, source_thread_id, event="task_created_from_here",
                                           text=f"{actor.user.name} created a task: {clean_title}",
                                           data={"taskId": task_id, "messageId": source_message_id})
        if owner_id != actor.user_id:
            await self.notifier.notify(cid, [owner_id], kind="task_assigned", actor_id=actor.user_id,
                                       title=f"{actor.user.name} assigned you a task", body=clean_title,
                                       task_id=task_id, thread_id=thread_id)
        if watchers:
            await self.notifier.notify(cid, watchers, kind="task_watching", actor_id=actor.user_id,
                                       title=f"{actor.user.name} added you as a watcher", body=clean_title,
                                       task_id=task_id, thread_id=thread_id)
        await self._publish(task)
        await self.audit.record(actor.user_id, cid, "task.created", task_id=task_id, thread_id=thread_id,
                                targets=participants, anchorKey=anchor.key if anchor else None)
        return await self.get(actor, task_id)

    # ──────────────────────────────────────────────────────────── reading ──
    async def _visible_task(self, actor: Actor, task_id: str) -> Task:
        task = await self.d.store.get_task(actor.customer_id, task_id) if is_ulid(task_id) else None
        if task is None or task.customerId != actor.customer_id:
            raise NotFound("That task does not exist.")
        if actor.user_id not in task.participants() and not actor.user.canViewTeamTasks:
            raise NotFound("That task does not exist.")
        return task

    async def get(self, actor: Actor, task_id: str) -> dict:
        task = await self._visible_task(actor, task_id)
        events = await self.d.store.list_task_events(actor.customer_id, task_id)
        context = None
        if task.anchor is not None:
            if await self.access.can_see_anchor(actor, task.anchor):
                ctx = await self.d.store.get_context(actor.customer_id, task.threadId)
                context = {"visible": True, "anchor": views.anchor(task.anchor),
                           "snapshot": ctx.snapshot if ctx else None,
                           "capturedAt": ctx.capturedAt.isoformat() if ctx else None}
            else:
                await self.audit.record(actor.user_id, actor.customer_id, "context.hidden", task_id=task_id,
                                        anchorKey=task.anchor.key)
                context = {"visible": False, "kind": task.anchor.kind.value,
                           "message": "The item this task is about is no longer available to you."}
        ids = task.participants() | {e.actorId for e in events if e.actorId}
        users = await self.access.people(actor.customer_id, ids)
        now = self.d.clock.now()
        return {
            **views.task_summary(task, viewer_id=actor.user_id, now=now),
            "description": task.description,
            "needsApproval": task.needsApproval,
            "createdAt": task.createdAt.isoformat(),
            "statusChangedAt": task.statusChangedAt.isoformat(),
            "sourceThreadId": task.sourceThreadId if actor.user_id in task.participants() else None,
            "context": context,
            "history": [views.task_event(e) for e in events],
            "permissions": {
                "transitions": sorted(s.value for s in allowed_transitions(task, actor.user_id)),
                "edit": actor.user_id == task.creatorId,
                "requestUpdate": self._can_request_update(actor, task),
            },
            "people": views.people(users, ids),
        }

    async def list(self, actor: Actor, *, view: str = "assigned_to_me", status: Optional[str] = None,
                   due: Optional[str] = None, priority: Optional[str] = None, source: Optional[str] = None,
                   person: Optional[int] = None, q: Optional[str] = None) -> dict:
        tasks = await self._view_tasks(actor, view)
        now = self.d.clock.now()
        summary = self._summary(tasks, now)

        if status == "open":
            tasks = [t for t in tasks if t.is_open() or t.awaiting_approval()]
        elif status == "finished":
            tasks = [t for t in tasks if not t.is_open() and not t.awaiting_approval()]
        elif status:
            wanted = set(status.split(","))
            if not wanted <= {s.value for s in TaskStatus}:
                raise Invalid("Unknown status filter.", code="invalid_filter")
            tasks = [t for t in tasks if t.status.value in wanted]
        if due:
            tasks = [t for t in tasks if self._due_matches(t, due, now)]
        if priority:
            tasks = [t for t in tasks if t.priority.value in set(priority.split(","))]
        if source == "none":
            tasks = [t for t in tasks if t.anchor is None]
        elif source:
            tasks = [t for t in tasks if t.anchor is not None and t.anchor.kind.value == source]
        if person is not None:
            tasks = [t for t in tasks if t.ownerId == person]
        if q:
            needle = q.strip().lower()
            tasks = [t for t in tasks if needle in t.title.lower()]

        order = {TaskPriority.URGENT: 0, TaskPriority.HIGH: 1, TaskPriority.NORMAL: 2, TaskPriority.LOW: 3}
        far = datetime.max.replace(tzinfo=now.tzinfo)
        tasks.sort(key=lambda t: (not t.is_overdue(now), not (t.is_open() or t.awaiting_approval()),
                                  t.dueAt or far, order[t.priority], -t.updatedAt.timestamp()))
        ids = {i for t in tasks for i in (t.ownerId, t.creatorId)}
        users = await self.access.people(actor.customer_id, ids)
        return {"view": view, "summary": summary,
                "items": [views.task_summary(t, viewer_id=actor.user_id, now=now) for t in tasks],
                "people": views.people(users, ids)}

    async def counts_by_anchor(self, actor: Actor, keys: list[str]) -> dict:
        wanted = set(keys[:100])
        if actor.user.canViewTeamTasks:
            tasks = await self.d.store.list_task_index(actor.customer_id, TASK_INDEX_ALL)
        else:
            seen: dict[str, Task] = {}
            for part in (TASK_INDEX_OWNER, TASK_INDEX_CREATOR, TASK_INDEX_WATCHER):
                for t in await self.d.store.list_task_index(actor.customer_id, part, actor.user_id):
                    seen[t.id] = t
            tasks = list(seen.values())
        counts = {k: 0 for k in wanted}
        for t in tasks:
            if t.anchor is not None and t.anchor.key in wanted and (t.is_open() or t.awaiting_approval()):
                counts[t.anchor.key] += 1
        return {"counts": counts}

    async def _view_tasks(self, actor: Actor, view: str) -> list[Task]:
        if view not in VIEWS:
            raise Invalid("Unknown task view.", code="invalid_view")
        store, cid, uid = self.d.store, actor.customer_id, actor.user_id
        if view == "assigned_to_me":
            return await store.list_task_index(cid, TASK_INDEX_OWNER, uid)
        if view == "assigned_by_me":
            return await store.list_task_index(cid, TASK_INDEX_CREATOR, uid)
        if view == "watching":
            return await store.list_task_index(cid, TASK_INDEX_WATCHER, uid)
        if not actor.user.canViewTeamTasks:
            raise Forbidden("You do not have access to the team view.", code="team_view_forbidden")
        return await store.list_task_index(cid, TASK_INDEX_ALL)

    @staticmethod
    def _summary(tasks: list[Task], now: datetime) -> dict:
        week_end = now + timedelta(days=7)
        week_start = now - timedelta(days=7)
        return {
            "overdue": sum(1 for t in tasks if t.is_overdue(now)),
            "dueThisWeek": sum(1 for t in tasks if t.is_open() and t.dueAt and now <= t.dueAt <= week_end),
            "blocked": sum(1 for t in tasks if t.status == TaskStatus.BLOCKED),
            "awaitingApproval": sum(1 for t in tasks if t.awaiting_approval()),
            "doneThisWeek": sum(1 for t in tasks if t.completedAt and t.completedAt >= week_start),
        }

    @staticmethod
    def _due_matches(t: Task, due: str, now: datetime) -> bool:
        if due == "overdue":
            return t.is_overdue(now)
        if due == "none":
            return t.dueAt is None
        if t.dueAt is None:
            return False
        if due == "today":
            return t.dueAt.date() == now.date()
        if due == "week":
            return now <= t.dueAt <= now + timedelta(days=7)
        raise Invalid("Unknown due filter.", code="invalid_filter")

    # ──────────────────────────────────────────────────────────── writing ──
    async def change_status(self, actor: Actor, task_id: str, *, to: str, note: Optional[str],
                            expected_version: int) -> dict:
        self.d.limiter.check("task.update", actor.customer_id, actor.user_id)
        try:
            target = TaskStatus(to)
        except ValueError:
            raise Invalid("Unknown status.", code="invalid_status")
        clean_note = clean_text(note, max_chars=1000, field="note", allow_empty=True) or None
        task = await self._visible_task(actor, task_id)
        if actor.user_id not in (task.ownerId, task.creatorId):
            raise Forbidden("Only the owner or the person who assigned this task can change its status.",
                            code="transition_not_allowed")
        now = self.d.clock.now()

        def mutate(t: Task) -> Task:
            if t.version != expected_version:
                raise Conflict("This task changed since you opened it. Refresh to see the latest.",
                               code="version_conflict", currentVersion=t.version)
            if target not in allowed_transitions(t, actor.user_id):
                raise Forbidden(f"You cannot move this task from {STATUS_LABEL[t.status]} to {STATUS_LABEL[target]}.",
                                code="transition_not_allowed")
            if note_required(t, target, actor.user_id) and not clean_note:
                raise Invalid("Add a note explaining why.", code="note_required")
            t.status = target
            t.statusChangedAt = t.updatedAt = t.lastActivityAt = now
            t.version += 1
            if target in (TaskStatus.DONE, TaskStatus.CLOSED):
                t.completedAt = t.completedAt or now
            else:
                t.completedAt = None
            t.reminders.staleNoticeAt = None
            t.reminders.blockedEscalatedAt = None
            t.reminders.approvalReminderAt = None
            return t

        updated, previous = await self.d.store.update_task(actor.customer_id, task_id, mutate)
        await self.d.store.index_task(updated, previous)
        await self._event(updated, actor.user_id, "status_changed", from_status=previous.status,
                          to_status=target, note=clean_note)

        verb = self._status_sentence(actor.user.name, previous.status, target, updated)
        text = f"{verb}: {clean_note}" if clean_note else verb
        await self.threads.post_system(actor.customer_id, updated.threadId, event="task_status",
                                       text=text, data={"from": previous.status.value, "to": target.value,
                                                        "by": actor.user_id})
        await self._notify_status(actor, previous, updated, text)
        await self._publish(updated)
        await self.audit.record(actor.user_id, actor.customer_id, "task.status_changed", task_id=task_id,
                                thread_id=updated.threadId, fromStatus=previous.status.value, toStatus=target.value)
        return await self.get(actor, task_id)

    async def update(self, actor: Actor, task_id: str, *, changes: dict[str, Any], expected_version: int) -> dict:
        self.d.limiter.check("task.update", actor.customer_id, actor.user_id)
        task = await self._visible_task(actor, task_id)
        if actor.user_id != task.creatorId:
            raise Forbidden("Only the person who assigned this task can edit it.", code="edit_not_allowed")
        allowed = {"title", "description", "dueAt", "priority", "ownerId", "watcherIds", "needsApproval"}
        unknown = set(changes) - allowed
        if unknown:
            raise Invalid(f"These fields cannot be changed: {', '.join(sorted(unknown))}.", code="invalid_fields")
        now = self.d.clock.now()
        cid = actor.customer_id

        new: dict[str, Any] = {}
        if "title" in changes:
            new["title"] = clean_text(changes["title"], max_chars=200, field="title")
        if "description" in changes:
            new["description"] = clean_text(changes["description"], max_chars=4000, field="description", allow_empty=True)
        if "dueAt" in changes:
            new["dueAt"] = self._check_due(changes["dueAt"], now)
        if "priority" in changes:
            try:
                new["priority"] = TaskPriority(changes["priority"])
            except ValueError:
                raise Invalid("Unknown priority.", code="invalid_priority")
        if "needsApproval" in changes:
            new["needsApproval"] = bool(changes["needsApproval"])
        owner_id = int(changes["ownerId"]) if "ownerId" in changes else task.ownerId
        if "ownerId" in changes:
            new["ownerId"] = owner_id
        if "watcherIds" in changes or "ownerId" in changes:
            base = changes.get("watcherIds", task.watcherIds)
            watchers = sorted({int(w) for w in base} - {owner_id, task.creatorId})
            if len(watchers) > MAX_WATCHERS:
                raise Invalid(f"A task can have at most {MAX_WATCHERS} watchers.", code="too_many_watchers")
            new["watcherIds"] = watchers

        added = ({new.get("ownerId", task.ownerId), *new.get("watcherIds", task.watcherIds)}
                 - task.participants())
        if added:
            await self.access.require_colleagues(actor, added)
            if task.anchor is not None:
                await self.access.require_anchor_access(cid, [actor.user_id, *added], task.anchor, actor_id=actor.user_id)

        def mutate(t: Task) -> Task:
            if t.version != expected_version:
                raise Conflict("This task changed since you opened it. Refresh to see the latest.",
                               code="version_conflict", currentVersion=t.version)
            for field, value in new.items():
                setattr(t, field, value)
            if "dueAt" in new:
                t.reminders.dueSoonFor = None
                t.reminders.overdueNoticeOn = None
            t.updatedAt = t.lastActivityAt = now
            t.version += 1
            return t

        updated, previous = await self.d.store.update_task(cid, task_id, mutate)
        diff = {k: {"from": _plain(getattr(previous, k)), "to": _plain(getattr(updated, k))}
                for k in new if getattr(previous, k) != getattr(updated, k)}
        if not diff:
            return await self.get(actor, task_id)
        await self.d.store.index_task(updated, previous)
        await self._sync_task_members(actor, previous, updated)
        await self._event(updated, actor.user_id, "updated", changes=diff)
        await self.threads.post_system(cid, updated.threadId, event="task_updated",
                                       text=f"{actor.user.name} updated {', '.join(_field_label(k) for k in diff)}",
                                       data={"fields": sorted(diff), "by": actor.user_id})
        if updated.ownerId != previous.ownerId:
            await self.notifier.notify(cid, [updated.ownerId], kind="task_assigned", actor_id=actor.user_id,
                                       title=f"{actor.user.name} assigned you a task", body=updated.title,
                                       task_id=task_id, thread_id=updated.threadId)
            await self.notifier.notify(cid, [previous.ownerId], kind="task_reassigned", actor_id=actor.user_id,
                                       title=f"{actor.user.name} reassigned a task you owned", body=updated.title,
                                       task_id=task_id)
        new_watchers = set(updated.watcherIds) - set(previous.watcherIds)
        if new_watchers:
            await self.notifier.notify(cid, new_watchers, kind="task_watching", actor_id=actor.user_id,
                                       title=f"{actor.user.name} added you as a watcher", body=updated.title,
                                       task_id=task_id, thread_id=updated.threadId)
        if "dueAt" in diff and updated.ownerId == previous.ownerId:
            await self.notifier.notify(cid, [updated.ownerId], kind="task_due_changed", actor_id=actor.user_id,
                                       title="The due date of a task you own changed", body=updated.title,
                                       task_id=task_id, thread_id=updated.threadId)
        await self._publish(updated, extra=previous.participants())
        await self.audit.record(actor.user_id, cid, "task.updated", task_id=task_id, fields=sorted(diff),
                                targets=sorted(updated.participants() ^ previous.participants()))
        return await self.get(actor, task_id)

    async def request_update(self, actor: Actor, task_id: str, *, note: Optional[str]) -> dict:
        task = await self._visible_task(actor, task_id)
        if not self._can_request_update(actor, task):
            raise Forbidden("You cannot ask for an update on this task.", code="request_update_not_allowed")
        self.d.limiter.check("task.request_update", actor.customer_id, actor.user_id, task_id)
        clean_note = clean_text(note, max_chars=500, field="note", allow_empty=True) or None
        await self._event(task, actor.user_id, "update_requested", note=clean_note)
        if actor.user_id in task.participants():
            await self.threads.post_system(actor.customer_id, task.threadId, event="update_requested",
                                           text=f"{actor.user.name} asked for an update" + (f": {clean_note}" if clean_note else ""),
                                           data={"by": actor.user_id})
        await self.notifier.notify(actor.customer_id, [task.ownerId], kind="task_update_requested",
                                   actor_id=actor.user_id, title=f"{actor.user.name} asked for an update",
                                   body=clean_note or task.title, task_id=task_id, thread_id=task.threadId)
        await self.audit.record(actor.user_id, actor.customer_id, "task.update_requested", task_id=task_id,
                                targets=[task.ownerId])
        return {"requested": True}

    # ─────────────────────────────────────────────────────────── helpers ──
    @staticmethod
    def _can_request_update(actor: Actor, task: Task) -> bool:
        if actor.user_id == task.ownerId or not (task.is_open() or task.awaiting_approval()):
            return False
        return actor.user_id in (task.creatorId, *task.watcherIds) or actor.user.canViewTeamTasks

    @staticmethod
    def _check_due(due: Any, now: datetime) -> Optional[datetime]:
        if due is None or due == "":
            return None
        if isinstance(due, str):
            try:
                due = datetime.fromisoformat(due.replace("Z", "+00:00"))
            except ValueError:
                raise Invalid("The due date is not a valid date.", code="invalid_due")
        if due.tzinfo is None:
            raise Invalid("The due date needs a timezone.", code="invalid_due")
        if due < now - timedelta(days=1):
            raise Invalid("The due date is in the past.", code="invalid_due")
        return due

    async def _event(self, task: Task, actor_id: Optional[int], event: str, *, from_status=None,
                     to_status=None, note=None, changes=None) -> None:
        await self.d.store.append_task_event(task.customerId, TaskEvent(
            id=new_ulid(), taskId=task.id, at=self.d.clock.now(), actorId=actor_id, event=event,
            fromStatus=from_status, toStatus=to_status, note=note, changes=changes or {}))

    async def _publish(self, task: Task, extra=()) -> None:
        await self.d.realtime.publish(task.customerId, task.participants() | set(extra),
                                      {"type": "task.updated", "taskId": task.id, "version": task.version,
                                       "status": task.status.value})

    async def _sync_task_members(self, actor: Actor, previous: Task, updated: Task) -> None:
        """The task conversation's members follow the task's people."""
        cid = updated.customerId
        now = self.d.clock.now()
        joined = updated.participants() - previous.participants()
        left = previous.participants() - updated.participants()
        for uid in joined:
            existing = await self.d.store.get_member(cid, updated.threadId, uid)
            if existing is None:
                await self.d.store.put_member(cid, Member(threadId=updated.threadId, userId=uid,
                                                          joinedAt=now, addedBy=actor.user_id))
            else:
                def rejoin(m: Member) -> Member:
                    m.leftAt = None
                    return m
                await self.d.store.update_member(cid, updated.threadId, uid, rejoin)
        for uid in left:
            def depart(m: Member) -> Member:
                m.leftAt = m.leftAt or now
                return m
            try:
                await self.d.store.update_member(cid, updated.threadId, uid, depart)
            except KeyError:
                pass
            await self.d.store.update_inbox(cid, uid, updated.threadId, lambda _: None)
        if joined or left:
            count = sum(1 for m in await self.d.store.list_members(cid, updated.threadId) if m.active)

            def recount(h: ThreadHead) -> ThreadHead:
                h.memberCount = count
                return h
            await self.d.store.update_thread(cid, updated.threadId, recount)

    async def _notify_status(self, actor: Actor, previous: Task, task: Task, text: str) -> None:
        cid = task.customerId
        to = task.status
        common = dict(actor_id=actor.user_id, body=task.title, task_id=task.id, thread_id=task.threadId)
        if to == TaskStatus.DONE:
            kind = "task_awaiting_approval" if task.needsApproval else "task_done"
            await self.notifier.notify(cid, [task.creatorId, *task.watcherIds], kind=kind, title=text, **common)
        elif to == TaskStatus.BLOCKED:
            await self.notifier.notify(cid, [task.creatorId], kind="task_blocked", title=text, **common)
        elif to == TaskStatus.CLOSED:
            await self.notifier.notify(cid, [task.ownerId, *task.watcherIds], kind="task_closed", title=text, **common)
        elif to == TaskStatus.CANCELLED:
            await self.notifier.notify(cid, [task.ownerId, *task.watcherIds], kind="task_cancelled", title=text, **common)
        elif previous.status in (TaskStatus.DONE, TaskStatus.CLOSED, TaskStatus.CANCELLED):
            await self.notifier.notify(cid, [task.ownerId], kind="task_reopened", title=text, **common)

    @staticmethod
    def _status_sentence(name: str, before: TaskStatus, after: TaskStatus, task: Task) -> str:
        if after == TaskStatus.DONE:
            return f"{name} marked this done" + (" and it is waiting for approval" if task.needsApproval else "")
        if after == TaskStatus.CLOSED:
            return f"{name} approved and closed this task"
        if after == TaskStatus.CANCELLED:
            return f"{name} cancelled this task"
        if before in (TaskStatus.DONE, TaskStatus.CLOSED) and after == TaskStatus.IN_PROGRESS:
            return f"{name} reopened this task"
        if before == TaskStatus.CANCELLED:
            return f"{name} restored this task"
        return f"{name} moved this to {STATUS_LABEL[after]}"


def _plain(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def _field_label(field: str) -> str:
    return {"title": "the title", "description": "the description", "dueAt": "the due date",
            "priority": "the priority", "ownerId": "the owner", "watcherIds": "the watchers",
            "needsApproval": "approval"}.get(field, field)
