"""The storage contract.

Two implementations keep each other honest: `MemoryStore` (tests, local dev)
and `AzureStore` (Blob + Table). `tests/integration/test_store_contract.py` runs the
same assertions against both, so a behaviour the service relies on cannot
exist in one and not the other.

Concurrency model
-----------------
Nothing here is a transaction across documents; Blob and Table storage do not
offer one. Instead every document that more than one writer can touch is
changed through an `update_*` method that takes a *mutate* function:

    read (with etag) -> mutate(copy) -> write if-match etag -> on conflict, retry

A mutate function may raise to abort (for example, a task whose version has
moved on). It must be safe to call more than once.

Multi-document operations are ordered so that a crash part-way leaves either
nothing visible or something the next retry completes. The service layer
documents that ordering where it matters.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Callable, Optional

from ..models import (
    AuditEvent, ContextSnapshot, InboxEntry, Member, Message, Notification,
    Task, TaskEvent, ThreadHead,
)


class StoreError(Exception):
    pass


class AlreadyExists(StoreError):
    pass


class ConcurrencyConflict(StoreError):
    """Retries were exhausted. Rare; surfaced to the caller as a 409."""


class StoreUnavailable(StoreError):
    pass


# Task index partitions: which list a task appears in.
TASK_INDEX_OWNER = "own"
TASK_INDEX_CREATOR = "cre"
TASK_INDEX_WATCHER = "wat"
TASK_INDEX_ALL = "all"

MAX_RETRIES = 12


class Store(ABC):
    # ── tenants ─────────────────────────────────────────────────────────
    @abstractmethod
    async def register_tenant(self, customer_id: int) -> None:
        """Idempotent. Creates the tenant's container and tables if needed."""

    @abstractmethod
    async def list_tenants(self) -> list[int]: ...

    # ── threads ─────────────────────────────────────────────────────────
    @abstractmethod
    async def create_thread(self, head: ThreadHead) -> ThreadHead:
        """Raises AlreadyExists."""

    @abstractmethod
    async def get_thread(self, customer_id: int, thread_id: str) -> Optional[ThreadHead]: ...

    @abstractmethod
    async def update_thread(self, customer_id: int, thread_id: str,
                            mutate: Callable[[ThreadHead], ThreadHead]) -> ThreadHead: ...

    @abstractmethod
    async def put_context(self, customer_id: int, thread_id: str, context: ContextSnapshot) -> None:
        """Create-only: a context never changes. Raises AlreadyExists."""

    @abstractmethod
    async def get_context(self, customer_id: int, thread_id: str) -> Optional[ContextSnapshot]: ...

    # ── members ─────────────────────────────────────────────────────────
    @abstractmethod
    async def put_member(self, customer_id: int, member: Member) -> Member: ...

    @abstractmethod
    async def get_member(self, customer_id: int, thread_id: str, user_id: int) -> Optional[Member]: ...

    @abstractmethod
    async def list_members(self, customer_id: int, thread_id: str) -> list[Member]: ...

    @abstractmethod
    async def update_member(self, customer_id: int, thread_id: str, user_id: int,
                            mutate: Callable[[Member], Member]) -> Member: ...

    # ── messages ────────────────────────────────────────────────────────
    @abstractmethod
    async def claim_message_id(self, customer_id: int, thread_id: str, message_id: str, seq: int) -> Optional[int]:
        """Record that `message_id` owns `seq`. Returns None if this call made
        the claim, or the seq an earlier claim recorded."""

    @abstractmethod
    async def find_message_seq(self, customer_id: int, thread_id: str, message_id: str) -> Optional[int]: ...

    @abstractmethod
    async def append_message(self, customer_id: int, message: Message) -> None: ...

    @abstractmethod
    async def list_messages(self, customer_id: int, thread_id: str, *, from_seq: int, to_seq: int) -> list[Message]:
        """Inclusive range, ascending by seq, tombstones already applied.
        Sequence numbers may have gaps (a failed send can burn one)."""

    @abstractmethod
    async def put_tombstone(self, customer_id: int, thread_id: str, message_id: str,
                            deleted_by: int, deleted_at: datetime) -> None: ...

    # ── inbox ───────────────────────────────────────────────────────────
    @abstractmethod
    async def get_inbox(self, customer_id: int, user_id: int, thread_id: str) -> Optional[InboxEntry]: ...

    @abstractmethod
    async def update_inbox(self, customer_id: int, user_id: int, thread_id: str,
                           mutate: Callable[[Optional[InboxEntry]], Optional[InboxEntry]]) -> Optional[InboxEntry]:
        """mutate receives None when no entry exists; returning None deletes it."""

    @abstractmethod
    async def list_inbox(self, customer_id: int, user_id: int, *, limit: int,
                         cursor: Optional[str] = None) -> tuple[list[InboxEntry], Optional[str]]:
        """Most recent activity first."""

    # ── idempotency ─────────────────────────────────────────────────────
    @abstractmethod
    async def claim_idempotency(self, customer_id: int, user_id: int, key: str, value: str) -> Optional[str]:
        """None if this call stored `value`; otherwise the value stored first."""

    # ── tasks ───────────────────────────────────────────────────────────
    @abstractmethod
    async def create_task(self, task: Task) -> Task: ...

    @abstractmethod
    async def get_task(self, customer_id: int, task_id: str) -> Optional[Task]: ...

    @abstractmethod
    async def update_task(self, customer_id: int, task_id: str,
                          mutate: Callable[[Task], Task]) -> tuple[Task, Task]:
        """Returns (updated, previous)."""

    @abstractmethod
    async def index_task(self, task: Task, previous: Optional[Task] = None) -> None:
        """Place the task in every list it belongs to and remove it from lists
        it has left. The index is derived and can always be rebuilt."""

    @abstractmethod
    async def list_task_index(self, customer_id: int, partition: str, user_id: Optional[int] = None) -> list[Task]: ...

    @abstractmethod
    async def append_task_event(self, customer_id: int, event: TaskEvent) -> None: ...

    @abstractmethod
    async def list_task_events(self, customer_id: int, task_id: str) -> list[TaskEvent]: ...

    # ── notifications ───────────────────────────────────────────────────
    @abstractmethod
    async def add_notification(self, customer_id: int, notification: Notification) -> None: ...

    @abstractmethod
    async def list_notifications(self, customer_id: int, user_id: int, *, limit: int) -> list[Notification]:
        """Newest first."""

    @abstractmethod
    async def mark_notifications_read(self, customer_id: int, user_id: int,
                                      ids: Optional[list[str]], at: datetime) -> int:
        """ids=None marks everything in the recent window read."""

    # ── audit ───────────────────────────────────────────────────────────
    @abstractmethod
    async def append_audit(self, event: AuditEvent) -> None: ...

    @abstractmethod
    async def list_audit(self, customer_id: int, day: date) -> list[AuditEvent]: ...

    async def close(self) -> None:
        return None
