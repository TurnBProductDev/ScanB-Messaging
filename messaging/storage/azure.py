"""Azure Blob + Table storage.

Layout, per customer (tenancy rule: one customer, one container):

  Blob container  msg-c{customerId}
    threads/{threadId}/context.json               create-only
    threads/{threadId}/messages/{segment}.jsonl   append blobs, `segment_size` messages each
    tasks/{taskId}/events.jsonl                   append blob
    audit/{yyyy}/{mm}/{dd}/{hh}.jsonl             append blobs; put an immutability policy on audit/

  Table  MsgC{customerId}          PartitionKey / RowKey
    th_{threadId} / head            thread head        (etag-guarded)
    th_{threadId} / m_{userId}      member             (etag-guarded)
    th_{threadId} / x_{messageId}   message id -> seq  (create-only: dedupes retries)
    th_{threadId} / d_{messageId}   tombstone
    ib_{userId}   / p_{threadId}    inbox pointer      (etag-guarded)
    ib_{userId}   / o_{inv}_{tid}   inbox, newest first; moved with its pointer in one transaction
    id_{userId}   / {key}           idempotency
    tk_{taskId}   / task            task               (etag-guarded)
    ti_{list}     / {taskId}        task lists: own_{u}, cre_{u}, wat_{u}, all
    nt_{userId}   / {inv}_{id}      notifications, newest first

  Table  MsgTenants  tenant / c{customerId}

Each entity carries its document as JSON in one `doc` property, so the two
stores persist byte-for-byte the same shapes.

Credentials: managed identity (DefaultAzureCredential) against the account
URLs. A connection string is for Azurite and local testing only; settings
refuse one in production.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import date, datetime
from typing import Callable, Optional, Type, TypeVar

from azure.core import MatchConditions
from azure.core.exceptions import (
    HttpResponseError, ResourceExistsError, ResourceModifiedError, ResourceNotFoundError,
    ServiceRequestError, ServiceResponseError,
)
from azure.data.tables import TableTransactionError, UpdateMode
from azure.data.tables.aio import TableServiceClient
from azure.storage.blob.aio import BlobServiceClient

from ..models import (
    AuditEvent, ContextSnapshot, Doc, InboxEntry, Member, Message, Notification,
    Task, TaskEvent, ThreadHead,
)
from . import keys
from .base import (
    AlreadyExists, ConcurrencyConflict, MAX_RETRIES, Store, StoreError, StoreUnavailable,
    TASK_INDEX_ALL,
)
from .memory import apply_tombstones, task_partitions

logger = logging.getLogger("messaging.storage.azure")

D = TypeVar("D", bound=Doc)
_MAX_DOC_CHARS = 30_000     # Table string properties top out at 32K UTF-16 characters


async def _backoff(attempt: int) -> None:
    """Full jitter: writers that collided must not retry in lockstep, or they
    collide again on every attempt."""
    await asyncio.sleep(random.uniform(0, min(0.5, 0.01 * (2 ** attempt))))


def _is_precondition_failure(exc: Exception) -> bool:
    return isinstance(exc, ResourceModifiedError) or (
        isinstance(exc, HttpResponseError) and getattr(exc, "status_code", None) == 412)


class AzureStore(Store):
    def __init__(self, *, blob_service: BlobServiceClient, table_service: TableServiceClient,
                 segment_size: int = 200, credential=None, namespace: str = "") -> None:
        # namespace prefixes every container and table name. Empty in every
        # real deployment; tests use it to give each run its own storage.
        self._ns = namespace.lower()
        self._blobs = blob_service
        self._tables = table_service
        self._credential = credential
        self.segment_size = segment_size
        self._ready: set[int] = set()

    @classmethod
    def from_settings(cls, settings) -> "AzureStore":
        if settings.storage_connection_string:
            cs = settings.storage_connection_string
            return cls(blob_service=BlobServiceClient.from_connection_string(cs),
                       table_service=TableServiceClient.from_connection_string(cs),
                       segment_size=settings.segment_size)
        from azure.identity.aio import DefaultAzureCredential
        credential = DefaultAzureCredential()
        return cls(blob_service=BlobServiceClient(account_url=settings.storage_blob_url, credential=credential),
                   table_service=TableServiceClient(endpoint=settings.storage_table_url, credential=credential),
                   segment_size=settings.segment_size, credential=credential)

    async def close(self) -> None:
        await self._blobs.close()
        await self._tables.close()
        if self._credential is not None:
            await self._credential.close()

    # ── plumbing ────────────────────────────────────────────────────────
    def _table_name(self, customer_id: int) -> str:
        return (self._ns.capitalize() + keys.table_name(customer_id)) if self._ns else keys.table_name(customer_id)

    def _container_name(self, customer_id: int) -> str:
        return f"{self._ns}-{keys.container_name(customer_id)}" if self._ns else keys.container_name(customer_id)

    @property
    def _tenants_table(self) -> str:
        return (self._ns.capitalize() + keys.TENANTS_TABLE) if self._ns else keys.TENANTS_TABLE

    def _table(self, customer_id: int):
        return self._tables.get_table_client(self._table_name(customer_id))

    def _container(self, customer_id: int):
        return self._blobs.get_container_client(self._container_name(customer_id))

    @staticmethod
    def _entity(pk: str, rk: str, doc: Doc) -> dict:
        body = doc.model_dump_json()
        if len(body) > _MAX_DOC_CHARS:
            raise StoreError(f"document too large for {pk}/{rk}")
        return {"PartitionKey": pk, "RowKey": rk, "doc": body}

    @staticmethod
    def _doc(entity, cls: Type[D]) -> D:
        doc = cls.model_validate_json(entity["doc"])
        doc.etag = entity.metadata.get("etag")
        return doc

    async def _get(self, customer_id: int, pk: str, rk: str, cls: Type[D]) -> Optional[D]:
        try:
            return self._doc(await self._table(customer_id).get_entity(pk, rk), cls)
        except ResourceNotFoundError:
            return None
        except (ServiceRequestError, ServiceResponseError) as exc:
            raise StoreUnavailable(str(exc)) from exc

    async def _query(self, customer_id: int, flt: str, params: dict, limit: Optional[int] = None) -> list:
        found = []
        try:
            pager = self._table(customer_id).query_entities(
                flt, parameters=params, results_per_page=min(limit, 1000) if limit else 1000)
            async for entity in pager:
                found.append(entity)
                if limit is not None and len(found) >= limit:
                    break
        except ResourceNotFoundError:
            return []
        except (ServiceRequestError, ServiceResponseError) as exc:
            raise StoreUnavailable(str(exc)) from exc
        return found

    async def _update(self, customer_id: int, pk: str, rk: str, cls: Type[D],
                      mutate: Callable[[D], D]) -> tuple[D, D]:
        table = self._table(customer_id)
        for attempt in range(MAX_RETRIES):
            current = await self._get(customer_id, pk, rk, cls)
            if current is None:
                raise KeyError(rk)
            before = current.model_dump(mode="json")
            updated = mutate(current.model_copy(deep=True))
            if updated.model_dump(mode="json") == before:
                updated.etag = current.etag
                return updated, current            # nothing changed: no write, no contention
            try:
                meta = await table.update_entity(self._entity(pk, rk, updated), mode=UpdateMode.REPLACE,
                                                 etag=current.etag, match_condition=MatchConditions.IfNotModified)
                updated.etag = meta.get("etag")
                return updated, current
            except Exception as exc:
                if _is_precondition_failure(exc):
                    await _backoff(attempt)
                    continue
                raise
        raise ConcurrencyConflict(f"{pk}/{rk}")

    async def _append_line(self, customer_id: int, blob_name: str, line: str) -> None:
        blob = self._container(customer_id).get_blob_client(blob_name)
        data = line.encode("utf-8") + b"\n"
        try:
            await blob.append_block(data)
        except ResourceNotFoundError:
            try:
                await blob.create_append_blob(match_condition=MatchConditions.IfMissing)
            except ResourceExistsError:
                pass
            await blob.append_block(data)

    async def _read_lines(self, customer_id: int, blob_name: str) -> list[str]:
        try:
            downloader = await self._container(customer_id).get_blob_client(blob_name).download_blob()
            raw = await downloader.readall()
        except ResourceNotFoundError:
            return []
        return [line for line in raw.decode("utf-8").split("\n") if line.strip()]

    # ── tenants ─────────────────────────────────────────────────────────
    async def register_tenant(self, customer_id: int) -> None:
        if customer_id in self._ready:
            return
        try:
            await self._blobs.create_container(self._container_name(customer_id))
        except ResourceExistsError:
            pass
        for name in (self._table_name(customer_id), self._tenants_table):
            try:
                await self._tables.create_table(name)
            except ResourceExistsError:
                pass
        await self._tables.get_table_client(self._tenants_table).upsert_entity(
            {"PartitionKey": "tenant", "RowKey": f"c{customer_id}", "customerId": customer_id})
        self._ready.add(customer_id)

    async def list_tenants(self) -> list[int]:
        found = []
        try:
            async for e in self._tables.get_table_client(self._tenants_table).query_entities(
                    "PartitionKey eq 'tenant'"):
                found.append(int(e["customerId"]))
        except ResourceNotFoundError:
            return []
        return sorted(found)

    # ── threads ─────────────────────────────────────────────────────────
    async def create_thread(self, head: ThreadHead) -> ThreadHead:
        await self.register_tenant(head.customerId)
        try:
            meta = await self._table(head.customerId).create_entity(self._entity(f"th_{head.id}", "head", head))
        except ResourceExistsError:
            raise AlreadyExists(head.id)
        stored = head.model_copy(deep=True)
        stored.etag = meta.get("etag")
        return stored

    async def get_thread(self, customer_id: int, thread_id: str) -> Optional[ThreadHead]:
        return await self._get(customer_id, f"th_{thread_id}", "head", ThreadHead)

    async def update_thread(self, customer_id, thread_id, mutate):
        await self.register_tenant(customer_id)
        updated, _ = await self._update(customer_id, f"th_{thread_id}", "head", ThreadHead, mutate)
        return updated

    async def put_context(self, customer_id: int, thread_id: str, context: ContextSnapshot) -> None:
        await self.register_tenant(customer_id)
        blob = self._container(customer_id).get_blob_client(keys.context_blob(thread_id))
        try:
            await blob.upload_blob(context.model_dump_json().encode("utf-8"), overwrite=False)
        except ResourceExistsError:
            raise AlreadyExists(thread_id)

    async def get_context(self, customer_id: int, thread_id: str) -> Optional[ContextSnapshot]:
        try:
            downloader = await self._container(customer_id).get_blob_client(keys.context_blob(thread_id)).download_blob()
            return ContextSnapshot.model_validate_json(await downloader.readall())
        except ResourceNotFoundError:
            return None

    # ── members ─────────────────────────────────────────────────────────
    async def put_member(self, customer_id: int, member: Member) -> Member:
        await self.register_tenant(customer_id)
        meta = await self._table(customer_id).upsert_entity(
            self._entity(f"th_{member.threadId}", f"m_{member.userId:010d}", member), mode=UpdateMode.REPLACE)
        stored = member.model_copy(deep=True)
        stored.etag = meta.get("etag")
        return stored

    async def get_member(self, customer_id, thread_id, user_id):
        return await self._get(customer_id, f"th_{thread_id}", f"m_{user_id:010d}", Member)

    async def list_members(self, customer_id, thread_id):
        entities = await self._query(customer_id, "PartitionKey eq @pk and RowKey ge 'm_' and RowKey lt 'm`'",
                                     {"pk": f"th_{thread_id}"})
        return [self._doc(e, Member) for e in entities]

    async def update_member(self, customer_id, thread_id, user_id, mutate):
        await self.register_tenant(customer_id)
        updated, _ = await self._update(customer_id, f"th_{thread_id}", f"m_{user_id:010d}", Member, mutate)
        return updated

    # ── messages ────────────────────────────────────────────────────────
    async def claim_message_id(self, customer_id, thread_id, message_id, seq):
        await self.register_tenant(customer_id)
        table = self._table(customer_id)
        try:
            await table.create_entity({"PartitionKey": f"th_{thread_id}", "RowKey": f"x_{message_id}", "seq": seq})
            return None
        except ResourceExistsError:
            existing = await table.get_entity(f"th_{thread_id}", f"x_{message_id}")
            return int(existing["seq"])

    async def find_message_seq(self, customer_id, thread_id, message_id):
        try:
            entity = await self._table(customer_id).get_entity(f"th_{thread_id}", f"x_{message_id}")
            return int(entity["seq"])
        except ResourceNotFoundError:
            return None

    async def append_message(self, customer_id: int, message: Message) -> None:
        await self.register_tenant(customer_id)
        segment = keys.segment_of(message.seq, self.segment_size)
        await self._append_line(customer_id, keys.segment_blob(message.threadId, segment), message.model_dump_json())

    async def list_messages(self, customer_id, thread_id, *, from_seq, to_seq):
        if to_seq < from_seq or to_seq < 1:
            return []
        first = keys.segment_of(max(from_seq, 1), self.segment_size)
        last = keys.segment_of(to_seq, self.segment_size)
        found: dict[str, Message] = {}
        for segment in range(first, last + 1):
            for line in await self._read_lines(customer_id, keys.segment_blob(thread_id, segment)):
                m = Message.model_validate_json(line)
                if from_seq <= m.seq <= to_seq:
                    found[m.id] = m        # a retried append can repeat a line; the id collapses it
        if not found:
            return []
        tombs = await self._query(customer_id, "PartitionKey eq @pk and RowKey ge 'd_' and RowKey lt 'd`'",
                                  {"pk": f"th_{thread_id}"})
        tombstones = {e["RowKey"][2:]: (int(e["by"]), e["at"]) for e in tombs}
        return apply_tombstones(sorted(found.values(), key=lambda m: m.seq), tombstones)

    async def put_tombstone(self, customer_id, thread_id, message_id, deleted_by, deleted_at):
        await self.register_tenant(customer_id)
        await self._table(customer_id).upsert_entity({
            "PartitionKey": f"th_{thread_id}", "RowKey": f"d_{message_id}",
            "by": deleted_by, "at": deleted_at})

    # ── inbox ───────────────────────────────────────────────────────────
    async def get_inbox(self, customer_id, user_id, thread_id):
        return await self._get(customer_id, f"ib_{user_id}", f"p_{thread_id}", InboxEntry)

    @staticmethod
    def _order_key(entry: InboxEntry) -> str:
        return keys.inbox_order_key(entry.lastMessageAt or datetime.fromtimestamp(0).astimezone(), entry.threadId)

    async def update_inbox(self, customer_id, user_id, thread_id, mutate):
        await self.register_tenant(customer_id)
        table = self._table(customer_id)
        pk, prk = f"ib_{user_id}", f"p_{thread_id}"
        skip_delete = False
        for attempt in range(MAX_RETRIES):
            current = await self._get(customer_id, pk, prk, InboxEntry)
            result = mutate(current.model_copy(deep=True) if current else None)
            old_rk = self._order_key(current) if current else None
            ops: list = []
            if result is None:
                if current is None:
                    return None
                ops.append(("delete", {"PartitionKey": pk, "RowKey": prk},
                            {"etag": current.etag, "match_condition": MatchConditions.IfNotModified}))
                if not skip_delete:
                    ops.append(("delete", {"PartitionKey": pk, "RowKey": old_rk}))
            else:
                if current is not None and result.model_dump(mode="json") == current.model_dump(mode="json"):
                    return current
                new_rk = self._order_key(result)
                pointer = self._entity(pk, prk, result)
                if current is None:
                    ops.append(("create", pointer))
                else:
                    ops.append(("update", pointer, {"mode": UpdateMode.REPLACE, "etag": current.etag,
                                                    "match_condition": MatchConditions.IfNotModified}))
                if old_rk and old_rk != new_rk and not skip_delete:
                    ops.append(("delete", {"PartitionKey": pk, "RowKey": old_rk}))
                ops.append(("upsert", self._entity(pk, new_rk, result), {"mode": UpdateMode.REPLACE}))
            try:
                await table.submit_transaction(ops)
                return result
            except TableTransactionError as exc:
                code = getattr(exc, "error_code", None) or ""
                if "ResourceNotFound" in str(code) or "ResourceNotFound" in str(exc):
                    skip_delete = True      # the ordered row was already gone; repair without it
                    continue
                if any(c in str(code) + str(exc) for c in ("UpdateConditionNotSatisfied", "EntityAlreadyExists")):
                    await _backoff(attempt)
                    continue
                raise
            except ResourceNotFoundError:
                skip_delete = True
                continue
            except Exception as exc:
                if _is_precondition_failure(exc) or isinstance(exc, ResourceExistsError):
                    await _backoff(attempt)
                    continue
                raise
        raise ConcurrencyConflict(f"{pk}/{prk}")

    async def list_inbox(self, customer_id, user_id, *, limit, cursor=None):
        if cursor:
            flt, params = "PartitionKey eq @pk and RowKey gt @cursor and RowKey lt 'p'", {"pk": f"ib_{user_id}", "cursor": cursor}
        else:
            flt, params = "PartitionKey eq @pk and RowKey ge 'o_' and RowKey lt 'p'", {"pk": f"ib_{user_id}"}
        entities = await self._query(customer_id, flt, params, limit=limit + 1)
        page = entities[:limit]
        next_cursor = page[-1]["RowKey"] if len(entities) > limit else None
        return [self._doc(e, InboxEntry) for e in page], next_cursor

    # ── idempotency ─────────────────────────────────────────────────────
    async def claim_idempotency(self, customer_id, user_id, key, value):
        await self.register_tenant(customer_id)
        table = self._table(customer_id)
        try:
            await table.create_entity({"PartitionKey": f"id_{user_id}", "RowKey": key, "value": value})
            return None
        except ResourceExistsError:
            return (await table.get_entity(f"id_{user_id}", key))["value"]

    # ── tasks ───────────────────────────────────────────────────────────
    async def create_task(self, task: Task) -> Task:
        await self.register_tenant(task.customerId)
        try:
            meta = await self._table(task.customerId).create_entity(self._entity(f"tk_{task.id}", "task", task))
        except ResourceExistsError:
            raise AlreadyExists(task.id)
        stored = task.model_copy(deep=True)
        stored.etag = meta.get("etag")
        return stored

    async def get_task(self, customer_id, task_id):
        return await self._get(customer_id, f"tk_{task_id}", "task", Task)

    async def update_task(self, customer_id, task_id, mutate):
        await self.register_tenant(customer_id)
        return await self._update(customer_id, f"tk_{task_id}", "task", Task, mutate)

    @staticmethod
    def _index_pk(partition: str) -> str:
        return "ti_" + partition.replace(":", "_")

    async def index_task(self, task: Task, previous: Optional[Task] = None) -> None:
        await self.register_tenant(task.customerId)
        table = self._table(task.customerId)
        wanted = task_partitions(task)
        stale = (task_partitions(previous) if previous else set()) - wanted
        for partition in wanted:
            await table.upsert_entity(self._entity(self._index_pk(partition), task.id, task), mode=UpdateMode.REPLACE)
        for partition in stale:
            try:
                await table.delete_entity(self._index_pk(partition), task.id)
            except ResourceNotFoundError:
                pass

    async def list_task_index(self, customer_id, partition, user_id=None):
        name = partition if user_id is None else f"{partition}:{user_id}"
        entities = await self._query(customer_id, "PartitionKey eq @pk", {"pk": self._index_pk(name)})
        return [self._doc(e, Task) for e in entities]

    async def append_task_event(self, customer_id, event):
        await self.register_tenant(customer_id)
        await self._append_line(customer_id, keys.task_events_blob(event.taskId), event.model_dump_json())

    async def list_task_events(self, customer_id, task_id):
        events = [TaskEvent.model_validate_json(line) for line in await self._read_lines(customer_id, keys.task_events_blob(task_id))]
        return sorted({e.id: e for e in events}.values(), key=lambda e: (e.at, e.id))

    # ── notifications ───────────────────────────────────────────────────
    async def add_notification(self, customer_id, notification):
        await self.register_tenant(customer_id)
        await self._table(customer_id).upsert_entity(self._entity(
            f"nt_{notification.userId}", keys.notification_key(notification.createdAt, notification.id), notification))

    async def list_notifications(self, customer_id, user_id, *, limit):
        entities = await self._query(customer_id, "PartitionKey eq @pk", {"pk": f"nt_{user_id}"}, limit=limit)
        return [self._doc(e, Notification) for e in entities]

    async def mark_notifications_read(self, customer_id, user_id, ids, at):
        await self.register_tenant(customer_id)
        table = self._table(customer_id)
        changed = 0
        for entity in await self._query(customer_id, "PartitionKey eq @pk", {"pk": f"nt_{user_id}"}, limit=200):
            note = self._doc(entity, Notification)
            if note.readAt is None and (ids is None or note.id in ids):
                note.readAt = at
                await table.update_entity(self._entity(entity["PartitionKey"], entity["RowKey"], note), mode=UpdateMode.REPLACE)
                changed += 1
        return changed

    # ── audit ───────────────────────────────────────────────────────────
    async def append_audit(self, event: AuditEvent) -> None:
        await self.register_tenant(event.customerId)
        await self._append_line(event.customerId, keys.audit_blob(event.at), event.model_dump_json())

    async def list_audit(self, customer_id: int, day: date) -> list[AuditEvent]:
        events = []
        try:
            async for blob in self._container(customer_id).list_blobs(name_starts_with=keys.audit_day_prefix(day)):
                events += [AuditEvent.model_validate_json(line) for line in await self._read_lines(customer_id, blob.name)]
        except ResourceNotFoundError:
            return []
        return sorted(events, key=lambda e: (e.at, e.id))
