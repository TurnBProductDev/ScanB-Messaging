"""Domain model. Storage-agnostic: both stores persist exactly these shapes.

Field names are camelCase because these documents are also the API's and the
Web App's contract; translating names at every boundary is how two sides of a
seam drift apart.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from enum import StrEnum
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Doc(BaseModel):
    """Base for stored documents. `etag` is the store's concurrency token and is
    never serialised into the document itself."""
    model_config = ConfigDict(extra="forbid")
    etag: Optional[str] = Field(default=None, exclude=True)


# ─────────────────────────────────────────────────────────────── context ──
class AnchorKind(StrEnum):
    INSIGHT = "insight"
    SUMMARY = "summary"
    REPORT = "report"
    DASHBOARD = "dashboard"
    CHAT_ANSWER = "chat_answer"


class AnchorResolution(StrEnum):
    """How firmly the Web App tied the item to something access can be checked
    against. Recorded, so a weak tie is a fact rather than an accident."""
    REPORT = "report"
    MODULE = "module"
    TENANT_ONLY = "tenant-only"


_KEY_RE = re.compile(r"^[A-Za-z0-9:_\-.]{1,200}$")


class Anchor(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: AnchorKind
    resolution: AnchorResolution
    key: str                                  # stable identity, e.g. "insight:target_tracker:4:2026-09-02"
    title: str = Field(max_length=200)
    reportRowId: Optional[int] = None
    moduleId: Optional[int] = None
    pbReportId: Optional[str] = Field(default=None, max_length=64)
    reportSlug: Optional[str] = Field(default=None, max_length=100)
    dashboardKey: Optional[str] = Field(default=None, max_length=200)
    url: Optional[str] = Field(default=None, max_length=500)   # app-relative link to the live item

    @field_validator("key")
    @classmethod
    def _key(cls, v: str) -> str:
        if not _KEY_RE.match(v):
            raise ValueError("anchor key has invalid characters")
        return v

    @field_validator("url")
    @classmethod
    def _relative(cls, v: Optional[str]) -> Optional[str]:
        # App-relative only: a stored absolute URL would be an open redirect
        # sitting inside a message everyone trusts.
        if v is not None and (not v.startswith("/") or v.startswith("//") or v.startswith("/\\")):
            raise ValueError("url must be app-relative")
        return v


class ContextSnapshot(Doc):
    """The item as it looked when the conversation started. Never changes."""
    anchor: Anchor
    snapshot: dict[str, Any]
    capturedAt: datetime
    capturedBy: int
    verified: bool = True


# ─────────────────────────────────────────────────────────────── threads ──
class ThreadType(StrEnum):
    DIRECT = "direct"
    GROUP = "group"
    CONTEXT = "context"
    CHANNEL = "channel"
    TASK = "task"


class ThreadHead(Doc):
    id: str
    customerId: int
    type: ThreadType
    title: Optional[str] = Field(default=None, max_length=200)
    createdBy: int
    createdAt: datetime
    seq: int = 0                       # last sequence number issued
    lastUserSeq: int = 0               # last sequence number of a message a person wrote
    lastMessageAt: Optional[datetime] = None
    lastPreview: Optional[str] = None
    lastSenderId: Optional[int] = None
    memberCount: int = 0
    anchor: Optional[Anchor] = None
    taskId: Optional[str] = None
    status: Literal["active", "archived"] = "active"


class MemberRole(StrEnum):
    OWNER = "owner"
    MEMBER = "member"


class Member(Doc):
    threadId: str
    userId: int
    role: MemberRole = MemberRole.MEMBER
    joinedAt: datetime
    addedBy: int
    leftAt: Optional[datetime] = None
    lastReadSeq: int = 0
    muted: bool = False

    @property
    def active(self) -> bool:
        return self.leftAt is None


class MessageKind(StrEnum):
    USER = "user"
    SYSTEM = "system"


class Message(Doc):
    id: str
    threadId: str
    seq: int
    kind: MessageKind
    senderId: Optional[int] = None
    text: str = ""
    system: Optional[dict[str, Any]] = None     # {"event": "member_added", ...}
    createdAt: datetime
    deletedAt: Optional[datetime] = None
    deletedBy: Optional[int] = None


class InboxEntry(Doc):
    """One conversation in one person's list. Derived from the thread and the
    member row, so the store can always rebuild it; it is never the truth."""
    threadId: str
    type: ThreadType
    title: Optional[str] = None
    # The whole anchor, not just its kind: a conversation title defaulted from
    # an insight carries that insight's numbers, so the list has to be able to
    # re-check access and hide it exactly as the conversation view does.
    anchor: Optional[Anchor] = None
    taskId: Optional[str] = None
    lastMessageAt: Optional[datetime] = None
    lastPreview: Optional[str] = None
    lastSenderId: Optional[int] = None
    seq: int = 0
    lastUserSeq: int = 0
    lastReadSeq: int = 0
    muted: bool = False
    memberIds: list[int] = Field(default_factory=list)   # small threads only; names the conversation

    @property
    def unread(self) -> bool:
        return self.lastUserSeq > self.lastReadSeq


# ───────────────────────────────────────────────────────────────── tasks ──
class TaskStatus(StrEnum):
    TODO = "todo"
    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    DONE = "done"
    CLOSED = "closed"
    CANCELLED = "cancelled"


OPEN_STATUSES = frozenset({TaskStatus.TODO, TaskStatus.IN_PROGRESS, TaskStatus.BLOCKED})


class TaskPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class ReminderState(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dueSoonFor: Optional[datetime] = None     # the dueAt a due-soon reminder was sent for
    overdueNoticeOn: Optional[date] = None    # last calendar day an overdue notice went out
    staleNoticeAt: Optional[datetime] = None
    blockedEscalatedAt: Optional[datetime] = None
    approvalReminderAt: Optional[datetime] = None


class Task(Doc):
    id: str
    customerId: int
    threadId: str
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=4000)
    ownerId: int
    creatorId: int
    watcherIds: list[int] = Field(default_factory=list)
    status: TaskStatus = TaskStatus.TODO
    priority: TaskPriority = TaskPriority.NORMAL
    dueAt: Optional[datetime] = None
    needsApproval: bool = False
    anchor: Optional[Anchor] = None
    sourceThreadId: Optional[str] = None
    sourceMessageId: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    statusChangedAt: datetime
    lastActivityAt: datetime
    completedAt: Optional[datetime] = None
    version: int = 1
    reminders: ReminderState = Field(default_factory=ReminderState)

    def participants(self) -> set[int]:
        return {self.ownerId, self.creatorId, *self.watcherIds}

    def is_open(self) -> bool:
        return self.status in OPEN_STATUSES

    def awaiting_approval(self) -> bool:
        return self.needsApproval and self.status == TaskStatus.DONE

    def is_overdue(self, now: datetime) -> bool:
        return self.dueAt is not None and self.is_open() and self.dueAt < now


class TaskEvent(Doc):
    id: str
    taskId: str
    at: datetime
    actorId: Optional[int] = None             # None = the system (reminders)
    event: str                                # created | status_changed | updated | update_requested | reminder | escalated
    fromStatus: Optional[TaskStatus] = None
    toStatus: Optional[TaskStatus] = None
    note: Optional[str] = None
    changes: dict[str, Any] = Field(default_factory=dict)


# ───────────────────────────────────────────────────────── notifications ──
class Notification(Doc):
    """Activity addressed to one person: task assignments, reminders, update
    requests. Conversations make themselves unread; these are everything else."""
    id: str
    userId: int
    kind: str
    title: str = Field(max_length=200)
    body: str = Field(default="", max_length=500)
    taskId: Optional[str] = None
    threadId: Optional[str] = None
    actorId: Optional[int] = None
    createdAt: datetime
    readAt: Optional[datetime] = None


# ───────────────────────────────────────────────────────────────── audit ──
class AuditEvent(Doc):
    """Append-only. Carries ids, never message text: the audit trail records
    what happened, and the message store is the only copy of what was said."""
    id: str
    at: datetime
    customerId: int
    actorId: Optional[int]
    action: str
    threadId: Optional[str] = None
    messageId: Optional[str] = None
    taskId: Optional[str] = None
    targetUserIds: list[int] = Field(default_factory=list)
    detail: dict[str, Any] = Field(default_factory=dict)
