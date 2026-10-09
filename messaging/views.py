"""Response shapes. The API returns only what is built here.

People are returned once per response as a `people` map keyed by user id, not
repeated inside every message, and never with an email address: the picker
and the conversation need a name and a role, and an address list is not
something a messaging API should be able to leak.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional

from .directory import DirectoryUser
from .models import (
    Anchor, InboxEntry, Member, Message, MessageKind, Notification, Task,
    TaskEvent, ThreadHead,
)

HIDDEN_TITLE = "Conversation about an item you can no longer see"


def person(user: Optional[DirectoryUser], user_id: int) -> dict[str, Any]:
    if user is None:
        return {"userId": user_id, "name": "Unknown person", "role": None, "active": False}
    return {"userId": user.userId, "name": user.name, "role": user.role, "active": user.usable}


def people(users: dict[int, DirectoryUser], ids: Iterable[int]) -> dict[str, dict[str, Any]]:
    return {str(uid): person(users.get(uid), uid) for uid in sorted({i for i in ids if i is not None})}


def anchor(a: Optional[Anchor]) -> Optional[dict[str, Any]]:
    return None if a is None else a.model_dump(mode="json", exclude_none=True)


def message(m: Message) -> dict[str, Any]:
    deleted = m.deletedAt is not None
    return {
        "id": m.id,
        "threadId": m.threadId,
        "seq": m.seq,
        "kind": m.kind.value,
        "senderId": m.senderId,
        "text": "" if deleted else m.text,
        "system": m.system if m.kind == MessageKind.SYSTEM else None,
        "createdAt": m.createdAt.isoformat(),
        "deleted": deleted,
    }


def inbox(e: InboxEntry, *, anchor_visible: bool = True) -> dict[str, Any]:
    title = e.title
    if e.anchor is not None and not anchor_visible and (title is None or title == e.anchor.title):
        title = HIDDEN_TITLE
    return {
        "threadId": e.threadId,
        "type": e.type.value,
        "title": title,
        "anchorKind": e.anchor.kind.value if e.anchor else None,
        "taskId": e.taskId,
        "lastMessageAt": e.lastMessageAt.isoformat() if e.lastMessageAt else None,
        "lastPreview": e.lastPreview,
        "lastSenderId": e.lastSenderId,
        "seq": e.seq,
        "lastReadSeq": e.lastReadSeq,
        "unread": e.unread,
        "muted": e.muted,
        "memberIds": e.memberIds,
    }


def member(m: Member) -> dict[str, Any]:
    return {"userId": m.userId, "role": m.role.value, "joinedAt": m.joinedAt.isoformat(), "lastReadSeq": m.lastReadSeq}


def thread(head: ThreadHead, members: list[Member], me: Member, context: Optional[dict[str, Any]],
           users: dict[int, DirectoryUser], task: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    title = head.title
    if head.anchor is not None and context is not None and not context.get("visible") \
            and (title is None or title == head.anchor.title):
        title = HIDDEN_TITLE
    ids = [m.userId for m in members] + [head.createdBy]
    return {
        "id": head.id,
        "type": head.type.value,
        "title": title,
        "createdBy": head.createdBy,
        "createdAt": head.createdAt.isoformat(),
        "seq": head.seq,
        "lastMessageAt": head.lastMessageAt.isoformat() if head.lastMessageAt else None,
        "status": head.status,
        "members": [member(m) for m in members],
        "me": {"lastReadSeq": me.lastReadSeq, "muted": me.muted, "role": me.role.value},
        "context": context,
        "task": task,
        "people": people(users, ids),
    }


def task_summary(t: Task, *, viewer_id: int, now) -> dict[str, Any]:
    return {
        "id": t.id,
        "title": t.title,
        "status": t.status.value,
        "priority": t.priority.value,
        "dueAt": t.dueAt.isoformat() if t.dueAt else None,
        "overdue": t.is_overdue(now),
        "awaitingApproval": t.awaiting_approval(),
        "ownerId": t.ownerId,
        "creatorId": t.creatorId,
        "watcherIds": t.watcherIds,
        "anchorKind": t.anchor.kind.value if t.anchor else None,
        "anchorKey": t.anchor.key if t.anchor else None,
        "updatedAt": t.updatedAt.isoformat(),
        "completedAt": t.completedAt.isoformat() if t.completedAt else None,
        "version": t.version,
        # Team viewers can see a task without being in its conversation.
        "threadId": t.threadId if viewer_id in t.participants() else None,
    }


def task_event(e: TaskEvent) -> dict[str, Any]:
    return {
        "id": e.id,
        "at": e.at.isoformat(),
        "actorId": e.actorId,
        "event": e.event,
        "fromStatus": e.fromStatus.value if e.fromStatus else None,
        "toStatus": e.toStatus.value if e.toStatus else None,
        "note": e.note,
        "changes": e.changes,
    }


def notification(n: Notification) -> dict[str, Any]:
    return {
        "id": n.id,
        "kind": n.kind,
        "title": n.title,
        "body": n.body,
        "taskId": n.taskId,
        "threadId": n.threadId,
        "actorId": n.actorId,
        "createdAt": n.createdAt.isoformat(),
        "read": n.readAt is not None,
    }
