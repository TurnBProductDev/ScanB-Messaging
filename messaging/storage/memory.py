"""In-process store for tests and local development.

Deliberately copies on every read and write so a caller can never mutate
stored state by holding a reference, which is the class of bug a real store
would never show and an in-memory one would silently hide.
"""
from __future__ import annotations

import asyncio
import itertools
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Callable, Optional

from ..models import (
    AuditEvent, ContextSnapshot, InboxEntry, Member, Message, Notification,
    Task, TaskEvent, ThreadHead,
)
from . import keys
from .base import (
    AlreadyExists, Store, TASK_INDEX_ALL, TASK_INDEX_CREATOR, TASK_INDEX_OWNER,
    TASK_INDEX_WATCHER,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class _Tenant:
    def __init__(self) -> None:
        self.threads: dict[str, ThreadHead] = {}
        self.contexts: dict[str, ContextSnapshot] = {}
        self.members: dict[str, dict[int, Member]] = defaultdict(dict)
        self.message_ids: dict[str, dict[str, int]] = defaultdict(dict)
        self.messages: dict[str, dict[int, list[Message]]] = defaultdict(lambda: defaultdict(list))
        self.tombstones: dict[str, dict[str, tuple[int, datetime]]] = defaultdict(dict)
        self.inbox: dict[int, dict[str, InboxEntry]] = defaultdict(dict)
        self.idem: dict[tuple[int, str], str] = {}
        self.tasks: dict[str, Task] = {}
        self.task_index: dict[str, dict[str, Task]] = defaultdict(dict)
        self.task_events: dict[str, list[TaskEvent]] = defaultdict(list)
        self.notifications: dict[int, dict[str, Notification]] = defaultdict(dict)
        self.audit: list[AuditEvent] = []


class MemoryStore(Store):
    def __init__(self, segment_size: int = 200) -> None:
        self._tenants: dict[int, _Tenant] = defaultdict(_Tenant)
        self._registered: set[int] = set()
        self._etags = itertools.count(1)
        self._lock = asyncio.Lock()
        self.segment_size = segment_size

    # helpers ---------------------------------------------------------------
    def _t(self, customer_id: int) -> _Tenant:
        return self._tenants[customer_id]

    def _stamp(self, doc):
        copy = doc.model_copy(deep=True)
        copy.etag = str(next(self._etags))
        return copy

    @staticmethod
    def _copy(doc):
        return None if doc is None else doc.model_copy(deep=True)

    # tenants -----------------------------------------------------------------
    async def register_tenant(self, customer_id: int) -> None:
        self._registered.add(customer_id)

    async def list_tenants(self) -> list[int]:
        return sorted(self._registered)

    # threads -----------------------------------------------------------------
    async def create_thread(self, head: ThreadHead) -> ThreadHead:
        async with self._lock:
            t = self._t(head.customerId)
            if head.id in t.threads:
                raise AlreadyExists(head.id)
            t.threads[head.id] = self._stamp(head)
            return self._copy(t.threads[head.id])

    async def get_thread(self, customer_id: int, thread_id: str) -> Optional[ThreadHead]:
        return self._copy(self._t(customer_id).threads.get(thread_id))

    async def update_thread(self, customer_id: int, thread_id: str,
                            mutate: Callable[[ThreadHead], ThreadHead]) -> ThreadHead:
        async with self._lock:
            t = self._t(customer_id)
            current = t.threads.get(thread_id)
            if current is None:
                raise KeyError(thread_id)
            updated = mutate(self._copy(current))
            t.threads[thread_id] = self._stamp(updated)
            return self._copy(t.threads[thread_id])

    async def put_context(self, customer_id: int, thread_id: str, context: ContextSnapshot) -> None:
        t = self._t(customer_id)
        if thread_id in t.contexts:
            raise AlreadyExists(thread_id)
        t.contexts[thread_id] = self._stamp(context)

    async def get_context(self, customer_id: int, thread_id: str) -> Optional[ContextSnapshot]:
        return self._copy(self._t(customer_id).contexts.get(thread_id))

    # members -----------------------------------------------------------------
    async def put_member(self, customer_id: int, member: Member) -> Member:
        stored = self._stamp(member)
        self._t(customer_id).members[member.threadId][member.userId] = stored
        return self._copy(stored)

    async def get_member(self, customer_id: int, thread_id: str, user_id: int) -> Optional[Member]:
        return self._copy(self._t(customer_id).members[thread_id].get(user_id))

    async def list_members(self, customer_id: int, thread_id: str) -> list[Member]:
        members = self._t(customer_id).members[thread_id]
        return [self._copy(members[uid]) for uid in sorted(members)]

    async def update_member(self, customer_id: int, thread_id: str, user_id: int,
                            mutate: Callable[[Member], Member]) -> Member:
        async with self._lock:
            members = self._t(customer_id).members[thread_id]
            current = members.get(user_id)
            if current is None:
                raise KeyError(user_id)
            members[user_id] = self._stamp(mutate(self._copy(current)))
            return self._copy(members[user_id])

    # messages ----------------------------------------------------------------
    async def claim_message_id(self, customer_id: int, thread_id: str, message_id: str, seq: int) -> Optional[int]:
        ids = self._t(customer_id).message_ids[thread_id]
        if message_id in ids:
            return ids[message_id]
        ids[message_id] = seq
        return None

    async def find_message_seq(self, customer_id: int, thread_id: str, message_id: str) -> Optional[int]:
        return self._t(customer_id).message_ids[thread_id].get(message_id)

    async def append_message(self, customer_id: int, message: Message) -> None:
        segment = keys.segment_of(message.seq, self.segment_size)
        self._t(customer_id).messages[message.threadId][segment].append(self._copy(message))

    async def list_messages(self, customer_id: int, thread_id: str, *, from_seq: int, to_seq: int) -> list[Message]:
        t = self._t(customer_id)
        if to_seq < from_seq or to_seq < 1:
            return []
        first = keys.segment_of(max(from_seq, 1), self.segment_size)
        last = keys.segment_of(to_seq, self.segment_size)
        found: dict[str, Message] = {}
        for segment in range(first, last + 1):
            for m in t.messages[thread_id].get(segment, []):
                if from_seq <= m.seq <= to_seq:
                    found[m.id] = self._copy(m)
        return _apply_tombstones(sorted(found.values(), key=lambda m: m.seq), t.tombstones[thread_id])

    async def put_tombstone(self, customer_id: int, thread_id: str, message_id: str,
                            deleted_by: int, deleted_at: datetime) -> None:
        self._t(customer_id).tombstones[thread_id][message_id] = (deleted_by, deleted_at)

    # inbox -------------------------------------------------------------------
    async def get_inbox(self, customer_id: int, user_id: int, thread_id: str) -> Optional[InboxEntry]:
        return self._copy(self._t(customer_id).inbox[user_id].get(thread_id))

    async def update_inbox(self, customer_id: int, user_id: int, thread_id: str,
                           mutate: Callable[[Optional[InboxEntry]], Optional[InboxEntry]]) -> Optional[InboxEntry]:
        async with self._lock:
            entries = self._t(customer_id).inbox[user_id]
            result = mutate(self._copy(entries.get(thread_id)))
            if result is None:
                entries.pop(thread_id, None)
                return None
            entries[thread_id] = self._stamp(result)
            return self._copy(entries[thread_id])

    async def list_inbox(self, customer_id: int, user_id: int, *, limit: int,
                         cursor: Optional[str] = None) -> tuple[list[InboxEntry], Optional[str]]:
        keyed = sorted(
            (keys.inbox_order_key(e.lastMessageAt or _EPOCH, e.threadId), e)
            for e in self._t(customer_id).inbox[user_id].values()
        )
        if cursor:
            keyed = [(k, e) for k, e in keyed if k > cursor]
        page = keyed[:limit]
        next_cursor = page[-1][0] if len(keyed) > limit else None
        return [self._copy(e) for _, e in page], next_cursor

    # idempotency ---------------------------------------------------------------
    async def claim_idempotency(self, customer_id: int, user_id: int, key: str, value: str) -> Optional[str]:
        store = self._t(customer_id).idem
        if (user_id, key) in store:
            return store[(user_id, key)]
        store[(user_id, key)] = value
        return None

    # tasks ---------------------------------------------------------------------
    async def create_task(self, task: Task) -> Task:
        t = self._t(task.customerId)
        if task.id in t.tasks:
            raise AlreadyExists(task.id)
        t.tasks[task.id] = self._stamp(task)
        return self._copy(t.tasks[task.id])

    async def get_task(self, customer_id: int, task_id: str) -> Optional[Task]:
        return self._copy(self._t(customer_id).tasks.get(task_id))

    async def update_task(self, customer_id: int, task_id: str,
                          mutate: Callable[[Task], Task]) -> tuple[Task, Task]:
        async with self._lock:
            t = self._t(customer_id)
            current = t.tasks.get(task_id)
            if current is None:
                raise KeyError(task_id)
            previous = self._copy(current)
            t.tasks[task_id] = self._stamp(mutate(self._copy(current)))
            return self._copy(t.tasks[task_id]), previous

    async def index_task(self, task: Task, previous: Optional[Task] = None) -> None:
        t = self._t(task.customerId)
        wanted = _task_partitions(task)
        stale = (_task_partitions(previous) if previous else set()) - wanted
        for p in stale:
            t.task_index[p].pop(task.id, None)
        for p in wanted:
            t.task_index[p][task.id] = self._copy(task)

    async def list_task_index(self, customer_id: int, partition: str, user_id: Optional[int] = None) -> list[Task]:
        name = partition if user_id is None else f"{partition}:{user_id}"
        return [self._copy(x) for x in self._t(customer_id).task_index[name].values()]

    async def append_task_event(self, customer_id: int, event: TaskEvent) -> None:
        self._t(customer_id).task_events[event.taskId].append(self._copy(event))

    async def list_task_events(self, customer_id: int, task_id: str) -> list[TaskEvent]:
        return [self._copy(e) for e in self._t(customer_id).task_events[task_id]]

    # notifications -------------------------------------------------------------
    async def add_notification(self, customer_id: int, notification: Notification) -> None:
        self._t(customer_id).notifications[notification.userId][notification.id] = self._copy(notification)

    async def list_notifications(self, customer_id: int, user_id: int, *, limit: int) -> list[Notification]:
        items = sorted(
            self._t(customer_id).notifications[user_id].values(),
            key=lambda n: keys.notification_key(n.createdAt, n.id),
        )
        return [self._copy(n) for n in items[:limit]]

    async def mark_notifications_read(self, customer_id: int, user_id: int,
                                      ids: Optional[list[str]], at: datetime) -> int:
        changed = 0
        recent = await self.list_notifications(customer_id, user_id, limit=200)
        store = self._t(customer_id).notifications[user_id]
        for n in recent:
            if n.readAt is None and (ids is None or n.id in ids):
                store[n.id].readAt = at
                changed += 1
        return changed

    # audit ---------------------------------------------------------------------
    async def append_audit(self, event: AuditEvent) -> None:
        self._t(event.customerId).audit.append(self._copy(event))

    async def list_audit(self, customer_id: int, day: date) -> list[AuditEvent]:
        return [self._copy(e) for e in self._t(customer_id).audit if e.at.date() == day]


def _task_partitions(task: Task) -> set[str]:
    parts = {TASK_INDEX_ALL, f"{TASK_INDEX_OWNER}:{task.ownerId}", f"{TASK_INDEX_CREATOR}:{task.creatorId}"}
    parts.update(f"{TASK_INDEX_WATCHER}:{w}" for w in task.watcherIds)
    return parts


def task_partitions(task: Task) -> set[str]:
    return _task_partitions(task)


def _apply_tombstones(messages: list[Message], tombstones: dict[str, tuple[int, datetime]]) -> list[Message]:
    for m in messages:
        if m.id in tombstones:
            m.deletedBy, m.deletedAt = tombstones[m.id]
            m.text = ""
    return messages


apply_tombstones = _apply_tombstones
