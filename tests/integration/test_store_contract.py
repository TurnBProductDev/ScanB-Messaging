"""One contract, both stores. The memory store always runs; Azure runs when
Azurite is available (MSG_TEST_AZURITE=1)."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest

from messaging.ids import new_ulid
from messaging.models import (
    AuditEvent, ContextSnapshot, InboxEntry, Member, Message, MessageKind, Notification,
    Task, TaskEvent, ThreadHead, ThreadType,
)
from messaging.storage.base import AlreadyExists, TASK_INDEX_ALL, TASK_INDEX_OWNER, TASK_INDEX_WATCHER
from tests.store_factory import make_store

T0 = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)
CID = 7

azure_enabled = os.getenv("MSG_TEST_AZURITE") == "1" or os.getenv("MSG_TEST_STORE") == "azure"


@pytest.fixture(params=[
    pytest.param("memory", marks=pytest.mark.unit),
    pytest.param("azure", marks=[pytest.mark.integration,
                                 pytest.mark.skipif(not azure_enabled, reason="set MSG_TEST_AZURITE=1 with Azurite running")]),
])
async def store(request):
    s = make_store(request.param, segment_size=3)
    await s.register_tenant(CID)
    yield s
    await s.close()


def head(tid=None, **kw):
    return ThreadHead(id=tid or new_ulid(), customerId=CID, type=ThreadType.GROUP, createdBy=1, createdAt=T0, **kw)


async def test_thread_create_get_and_duplicate(store):
    h = await store.create_thread(head(title="Range plan"))
    assert h.etag
    assert (await store.get_thread(CID, h.id)).title == "Range plan"
    with pytest.raises(AlreadyExists):
        await store.create_thread(head(h.id))
    assert await store.get_thread(CID, new_ulid()) is None
    assert await store.get_thread(999, h.id) is None           # another tenant sees nothing


async def test_concurrent_sequence_numbers_never_collide(store):
    h = await store.create_thread(head())

    def bump(x):
        x.seq += 1
        return x
    results = await asyncio.gather(*(store.update_thread(CID, h.id, bump) for _ in range(10)))
    assert sorted(r.seq for r in results) == list(range(1, 11))
    assert (await store.get_thread(CID, h.id)).seq == 10


async def test_context_is_create_only(store):
    from messaging.models import Anchor
    anchor = Anchor(kind="insight", resolution="report", key="insight:a:1:2026-09-15", title="T", reportRowId=1)
    ctx = ContextSnapshot(anchor=anchor, snapshot={"v": 1}, capturedAt=T0, capturedBy=1)
    await store.put_context(CID, "01HZZZZZZZZZZZZZZZZZZZZZZZ", ctx)
    with pytest.raises(AlreadyExists):
        await store.put_context(CID, "01HZZZZZZZZZZZZZZZZZZZZZZZ", ctx)
    assert (await store.get_context(CID, "01HZZZZZZZZZZZZZZZZZZZZZZZ")).snapshot == {"v": 1}


async def test_members(store):
    tid = new_ulid()
    for uid in (3, 1, 2):
        await store.put_member(CID, Member(threadId=tid, userId=uid, joinedAt=T0, addedBy=1))
    assert [m.userId for m in await store.list_members(CID, tid)] == [1, 2, 3]

    def read(m):
        m.lastReadSeq = 5
        return m
    assert (await store.update_member(CID, tid, 2, read)).lastReadSeq == 5
    with pytest.raises(KeyError):
        await store.update_member(CID, tid, 99, read)


async def test_messages_segments_dedupe_and_tombstones(store):
    tid = new_ulid()
    ids = [new_ulid() for _ in range(7)]
    for seq, mid in enumerate(ids, start=1):
        assert await store.claim_message_id(CID, tid, mid, seq) is None
        await store.append_message(CID, Message(id=mid, threadId=tid, seq=seq, kind=MessageKind.USER,
                                                senderId=1, text=f"m{seq}", createdAt=T0))
    assert await store.claim_message_id(CID, tid, ids[0], 99) == 1
    assert await store.find_message_seq(CID, tid, ids[3]) == 4
    # a retried append writes the same line twice; reads must collapse it
    await store.append_message(CID, Message(id=ids[4], threadId=tid, seq=5, kind=MessageKind.USER,
                                            senderId=1, text="m5", createdAt=T0))
    await store.put_tombstone(CID, tid, ids[1], 1, T0)
    got = await store.list_messages(CID, tid, from_seq=2, to_seq=6)
    assert [m.seq for m in got] == [2, 3, 4, 5, 6]
    assert got[0].text == "" and got[0].deletedBy == 1


async def test_inbox_ordering_cursor_and_removal(store):
    for i, tid in enumerate(["A" * 26, "B" * 26, "C" * 26]):
        def make(_, i=i, tid=tid):
            return InboxEntry(threadId=tid, type=ThreadType.GROUP, lastMessageAt=T0 + timedelta(minutes=i), seq=i)
        await store.update_inbox(CID, 1, tid, make)
    page, cursor = await store.list_inbox(CID, 1, limit=2)
    assert [e.threadId[0] for e in page] == ["C", "B"] and cursor
    rest, cursor2 = await store.list_inbox(CID, 1, limit=2, cursor=cursor)
    assert [e.threadId[0] for e in rest] == ["A"] and cursor2 is None

    def bump(e):                                   # new activity on A moves it to the top
        e.lastMessageAt = T0 + timedelta(hours=1)
        e.seq = 10
        return e
    await store.update_inbox(CID, 1, "A" * 26, bump)
    page, _ = await store.list_inbox(CID, 1, limit=10)
    assert [e.threadId[0] for e in page] == ["A", "C", "B"]

    await store.update_inbox(CID, 1, "C" * 26, lambda e: None)
    page, _ = await store.list_inbox(CID, 1, limit=10)
    assert [e.threadId[0] for e in page] == ["A", "B"]
    assert await store.get_inbox(CID, 1, "C" * 26) is None


async def test_concurrent_inbox_updates_settle_on_newest(store):
    tid = "D" * 26

    def at(seq):
        def mutate(e):
            e = e or InboxEntry(threadId=tid, type=ThreadType.DIRECT)
            if seq >= e.seq:
                e.seq = seq
                e.lastMessageAt = T0 + timedelta(seconds=seq)
            return e
        return mutate
    await asyncio.gather(*(store.update_inbox(CID, 5, tid, at(s)) for s in (3, 1, 4, 2, 5)))
    page, _ = await store.list_inbox(CID, 5, limit=10)
    assert len(page) == 1 and page[0].seq == 5          # one row, never duplicated by the moves


async def test_idempotency(store):
    assert await store.claim_idempotency(CID, 1, "thread:abc12345", "X") is None
    assert await store.claim_idempotency(CID, 1, "thread:abc12345", "Y") == "X"
    assert await store.claim_idempotency(CID, 2, "thread:abc12345", "Z") is None


async def test_tasks_update_and_index_moves(store):
    task = Task(id=new_ulid(), customerId=CID, threadId=new_ulid(), title="Fix", ownerId=2, creatorId=1,
                watcherIds=[3], createdAt=T0, updatedAt=T0, statusChangedAt=T0, lastActivityAt=T0)
    task = await store.create_task(task)
    await store.index_task(task)
    assert [t.id for t in await store.list_task_index(CID, TASK_INDEX_OWNER, 2)] == [task.id]

    def reassign(t):
        t.ownerId, t.watcherIds, t.version = 4, [], t.version + 1
        return t
    updated, previous = await store.update_task(CID, task.id, reassign)
    assert previous.ownerId == 2 and updated.version == 2
    await store.index_task(updated, previous)
    assert await store.list_task_index(CID, TASK_INDEX_OWNER, 2) == []
    assert await store.list_task_index(CID, TASK_INDEX_WATCHER, 3) == []
    assert [t.ownerId for t in await store.list_task_index(CID, TASK_INDEX_ALL)] == [4]

    for i in range(3):
        await store.append_task_event(CID, TaskEvent(id=new_ulid(), taskId=task.id, at=T0 + timedelta(seconds=i), event=f"e{i}"))
    assert [e.event for e in await store.list_task_events(CID, task.id)] == ["e0", "e1", "e2"]


async def test_notifications_newest_first_and_mark_read(store):
    ids = []
    for i in range(3):
        n = Notification(id=new_ulid(), userId=9, kind="k", title=f"n{i}", createdAt=T0 + timedelta(seconds=i))
        ids.append(n.id)
        await store.add_notification(CID, n)
    same_ms = [Notification(id=new_ulid(), userId=9, kind="k", title=f"tie{i}", createdAt=T0 + timedelta(seconds=9))
               for i in range(2)]
    for n in same_ms:
        await store.add_notification(CID, n)
    got = await store.list_notifications(CID, 9, limit=10)
    assert [n.title for n in got] == ["tie1", "tie0", "n2", "n1", "n0"]
    assert await store.mark_notifications_read(CID, 9, [ids[0]], T0) == 1
    assert await store.mark_notifications_read(CID, 9, None, T0) == 4
    assert all(n.readAt for n in await store.list_notifications(CID, 9, limit=10))


async def test_audit_append_and_list_by_day(store):
    for hour in (1, 23):
        await store.append_audit(AuditEvent(id=new_ulid(), at=T0.replace(hour=hour), customerId=CID, actorId=1, action="a"))
    await store.append_audit(AuditEvent(id=new_ulid(), at=T0 + timedelta(days=1), customerId=CID, actorId=1, action="b"))
    assert [e.action for e in await store.list_audit(CID, T0.date())] == ["a", "a"]


async def test_tenants_are_listed(store):
    await store.register_tenant(CID)
    await store.register_tenant(8)
    assert {CID, 8} <= set(await store.list_tenants())
