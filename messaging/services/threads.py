"""Conversations: direct, group, and conversations about an item.

Write ordering
--------------
Storage offers no transaction across documents, so every multi-step write is
ordered so that a crash part-way is either invisible or completed by a retry:

  start a conversation   context -> members -> thread head (commit point) -> inbox rows
  send a message         claim id -> issue seq -> append -> read pointer -> inbox rows -> live event

A thread with members but no head does not exist to any reader. A message id
that was claimed but never appended is completed by the client's retry, which
reuses the same message id. Inbox rows are derived and repairable, so they are
always written last.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

from .. import views
from ..context import verify as verify_context
from ..errors import Forbidden, Invalid, NotFound
from ..ids import direct_thread_id, is_thread_id, is_ulid, new_ulid
from ..models import (
    ContextSnapshot, InboxEntry, Member, MemberRole, Message, MessageKind,
    ThreadHead, ThreadType,
)
from ..storage.base import AlreadyExists
from ..text import clean_text, preview
from .common import Access, Actor, Auditor, Deps, Notifier

logger = logging.getLogger("messaging.threads")

_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")
_FANOUT_CONCURRENCY = 10
_SMALL_THREAD_NAMES = 10


def check_idempotency_key(key: Optional[str]) -> Optional[str]:
    if key is None:
        return None
    if not _IDEMPOTENCY_RE.match(key):
        raise Invalid("Idempotency-Key must be 8-128 letters, digits, '-' or '_'.", code="invalid_idempotency_key")
    return key


class ThreadService:
    def __init__(self, deps: Deps, access: Access, auditor: Auditor, notifier: Notifier) -> None:
        self.d = deps
        self.access = access
        self.audit = auditor
        self.notifier = notifier

    # ───────────────────────────────────────────────────────────── start ──
    async def start(self, actor: Actor, *, type: str, member_ids: list[int], title: Optional[str] = None,
                    context_token: Optional[str] = None, message_id: Optional[str] = None,
                    message_text: Optional[str] = None, idempotency_key: Optional[str] = None) -> dict:
        self.d.limiter.check("thread.create", actor.customer_id, actor.user_id)
        check_idempotency_key(idempotency_key)
        try:
            thread_type = ThreadType(type)
        except ValueError:
            raise Invalid("Unknown conversation type.", code="invalid_type")
        if thread_type not in (ThreadType.DIRECT, ThreadType.GROUP, ThreadType.CONTEXT):
            raise Invalid("That kind of conversation cannot be started here.", code="invalid_type")

        others = sorted({int(u) for u in member_ids if int(u) != actor.user_id})
        clean_title = clean_text(title, max_chars=200, field="title", allow_empty=True) or None
        verified = None

        if thread_type == ThreadType.DIRECT:
            if len(others) != 1 or context_token:
                raise Invalid("A direct conversation is between you and one other person.", code="invalid_members")
            thread_id = direct_thread_id(actor.user_id, others[0])
            clean_title = None
        else:
            if not others:
                raise Invalid("Choose at least one person.", code="invalid_members")
            if len(others) + 1 > self.d.settings.group_max_members:
                raise Invalid(f"A conversation can have at most {self.d.settings.group_max_members} people.",
                              code="too_many_members")
            if thread_type == ThreadType.CONTEXT:
                if not context_token:
                    raise Invalid("Attach the item this conversation is about.", code="context_required")
                verified = self._verify(actor, context_token)
                clean_title = clean_title or verified.anchor.title
            elif context_token:
                raise Invalid("Use a conversation about an item to attach one.", code="invalid_type")
            thread_id = new_ulid()

        await self.d.store.register_tenant(actor.customer_id)
        if idempotency_key and thread_type != ThreadType.DIRECT:
            prior = await self.d.store.claim_idempotency(actor.customer_id, actor.user_id,
                                                         f"thread:{idempotency_key}", thread_id)
            if prior is not None:
                thread_id = prior

        await self.access.require_colleagues(actor, others)
        if verified is not None:
            await self.access.require_anchor_access(actor.customer_id, [actor.user_id, *others],
                                                    verified.anchor, actor_id=actor.user_id)

        now = self.d.clock.now()
        head = await self.d.store.get_thread(actor.customer_id, thread_id)
        if head is None:
            if verified is not None:
                try:
                    await self.d.store.put_context(actor.customer_id, thread_id, ContextSnapshot(
                        anchor=verified.anchor, snapshot=verified.snapshot, capturedAt=now,
                        capturedBy=actor.user_id, verified=True))
                except AlreadyExists:
                    pass
            for uid in [actor.user_id, *others]:
                if await self.d.store.get_member(actor.customer_id, thread_id, uid) is None:
                    role = MemberRole.OWNER if uid == actor.user_id and thread_type != ThreadType.DIRECT else MemberRole.MEMBER
                    await self.d.store.put_member(actor.customer_id, Member(
                        threadId=thread_id, userId=uid, role=role, joinedAt=now, addedBy=actor.user_id))
            created = True
            try:
                head = await self.d.store.create_thread(ThreadHead(
                    id=thread_id, customerId=actor.customer_id, type=thread_type, title=clean_title,
                    createdBy=actor.user_id, createdAt=now, lastMessageAt=now, memberCount=len(others) + 1,
                    anchor=verified.anchor if verified else None))
            except AlreadyExists:
                created = False
                head = await self.d.store.get_thread(actor.customer_id, thread_id)
            members = await self.d.store.list_members(actor.customer_id, thread_id)
            await self._fanout(head, members)
            if created:
                await self.audit.record(actor.user_id, actor.customer_id, "thread.created", thread_id=thread_id,
                                        targets=others, type=thread_type.value,
                                        anchorKey=verified.anchor.key if verified else None)
                await self.d.realtime.publish(actor.customer_id, others, {"type": "thread.created", "threadId": thread_id})

        if message_text is not None:
            await self.post_message(actor, thread_id, message_id=message_id or new_ulid(), text=message_text)
        return await self.get_thread(actor, thread_id)

    def _verify(self, actor: Actor, token: str):
        return verify_context(token, self.d.settings.signing_keys(), now=self.d.clock.now(),
                              max_age_seconds=self.d.settings.context_max_age_seconds,
                              customer_id=actor.customer_id, user_id=actor.user_id)

    async def anchor_for(self, actor: Actor, *, context_token: Optional[str], thread_id: Optional[str]):
        """The item a people search is scoped to: a freshly attached one, or the
        one an existing conversation is about. None when there is neither."""
        if context_token:
            return self._verify(actor, context_token).anchor
        if thread_id:
            self._check_id(thread_id)
            head, _ = await self.access.require_member(actor, thread_id)
            return head.anchor
        return None

    # ─────────────────────────────────────────────────────────── reading ──
    async def get_thread(self, actor: Actor, thread_id: str) -> dict:
        self._check_id(thread_id)
        head, me = await self.access.require_member(actor, thread_id)
        members = [m for m in await self.d.store.list_members(actor.customer_id, thread_id) if m.active]
        context = await self._context_view(actor, head)
        task = None
        if head.taskId:
            t = await self.d.store.get_task(actor.customer_id, head.taskId)
            if t is not None:
                task = views.task_summary(t, viewer_id=actor.user_id, now=self.d.clock.now())
        users = await self.access.people(actor.customer_id, [m.userId for m in members] + [head.createdBy])
        return views.thread(head, members, me, context, users, task)

    async def _context_view(self, actor: Actor, head: ThreadHead) -> Optional[dict]:
        if head.anchor is None:
            return None
        if not await self.access.can_see_anchor(actor, head.anchor):
            await self.audit.record(actor.user_id, actor.customer_id, "context.hidden", thread_id=head.id,
                                    anchorKey=head.anchor.key)
            return {"visible": False, "kind": head.anchor.kind.value,
                    "message": "The item this conversation is about is no longer available to you."}
        ctx = await self.d.store.get_context(actor.customer_id, head.id)
        return {
            "visible": True,
            "anchor": views.anchor(head.anchor),
            "snapshot": ctx.snapshot if ctx else None,
            "capturedAt": ctx.capturedAt.isoformat() if ctx else None,
            "capturedBy": ctx.capturedBy if ctx else None,
            "verified": bool(ctx and ctx.verified),
        }

    async def list_messages(self, actor: Actor, thread_id: str, *, before_seq: Optional[int] = None,
                            limit: int = 50) -> dict:
        self._check_id(thread_id)
        head, _ = await self.access.require_member(actor, thread_id)
        limit = max(1, min(limit, 100))
        upper = head.seq if before_seq is None else min(head.seq, before_seq - 1)
        if upper < 1:
            return {"messages": [], "hasMore": False, "people": {}}
        lower = max(1, upper - limit + 1)
        found = await self.d.store.list_messages(actor.customer_id, thread_id, from_seq=lower, to_seq=upper)
        widen = 0
        while len(found) < limit and lower > 1 and widen < 3:   # sequence gaps from failed sends
            new_lower = max(1, lower - limit)
            found = await self.d.store.list_messages(actor.customer_id, thread_id,
                                                     from_seq=new_lower, to_seq=lower - 1) + found
            lower = new_lower
            widen += 1
        found = found[-limit:]
        has_more = bool(found) and found[0].seq > 1 or (not found and lower > 1)
        users = await self.access.people(actor.customer_id, [m.senderId for m in found])
        return {"messages": [views.message(m) for m in found], "hasMore": has_more,
                "people": views.people(users, [m.senderId for m in found])}

    async def inbox(self, actor: Actor, *, limit: int = 30, cursor: Optional[str] = None,
                    filter: str = "all") -> dict:
        limit = max(1, min(limit, 100))
        entries, next_cursor = await self.d.store.list_inbox(actor.customer_id, actor.user_id, limit=limit, cursor=cursor)
        if filter == "unread":
            entries = [e for e in entries if e.unread and not e.muted]
        elif filter in {t.value for t in ThreadType}:
            entries = [e for e in entries if e.type.value == filter]
        elif filter != "all":
            raise Invalid("Unknown filter.", code="invalid_filter")
        visible = await self._anchor_visibility(actor, entries)
        ids = {uid for e in entries for uid in e.memberIds} | {e.lastSenderId for e in entries if e.lastSenderId}
        users = await self.access.people(actor.customer_id, ids)
        return {"items": [views.inbox(e, anchor_visible=visible.get(e.threadId, True)) for e in entries],
                "nextCursor": next_cursor, "people": views.people(users, ids)}

    async def _anchor_visibility(self, actor: Actor, entries: list[InboxEntry]) -> dict[str, bool]:
        by_anchor: dict[str, bool] = {}
        result = {}
        for e in entries:
            if e.anchor is None:
                continue
            if e.anchor.key not in by_anchor:
                by_anchor[e.anchor.key] = await self.access.can_see_anchor(actor, e.anchor)
            result[e.threadId] = by_anchor[e.anchor.key]
        return result

    async def activity(self, actor: Actor) -> dict:
        """The bell: unread conversations and unread notifications, newest first."""
        entries, _ = await self.d.store.list_inbox(actor.customer_id, actor.user_id, limit=50)
        unread = [e for e in entries if e.unread and not e.muted]
        notes = await self.d.store.list_notifications(actor.customer_id, actor.user_id, limit=30)
        unread_notes = [n for n in notes if n.readAt is None]
        visible = await self._anchor_visibility(actor, unread[:10])

        items = [{"kind": "conversation", "at": e.lastMessageAt.isoformat() if e.lastMessageAt else None,
                  "conversation": views.inbox(e, anchor_visible=visible.get(e.threadId, True))} for e in unread[:10]]
        items += [{"kind": "notification", "at": n.createdAt.isoformat(), "notification": views.notification(n)}
                  for n in unread_notes[:10]]
        items.sort(key=lambda i: i["at"] or "", reverse=True)
        ids = {uid for e in unread[:10] for uid in e.memberIds} | {e.lastSenderId for e in unread[:10] if e.lastSenderId} \
            | {n.actorId for n in unread_notes[:10] if n.actorId}
        users = await self.access.people(actor.customer_id, ids)
        return {"unreadConversations": len(unread), "unreadNotifications": len(unread_notes),
                "items": items[:10], "people": views.people(users, ids)}

    async def mark_notifications_read(self, actor: Actor, ids: Optional[list[str]]) -> dict:
        changed = await self.d.store.mark_notifications_read(actor.customer_id, actor.user_id, ids, self.d.clock.now())
        return {"marked": changed}

    async def notifications(self, actor: Actor, *, limit: int = 50) -> dict:
        notes = await self.d.store.list_notifications(actor.customer_id, actor.user_id, limit=max(1, min(limit, 100)))
        users = await self.access.people(actor.customer_id, [n.actorId for n in notes])
        return {"items": [views.notification(n) for n in notes],
                "people": views.people(users, [n.actorId for n in notes])}

    # ─────────────────────────────────────────────────────────── writing ──
    async def post_message(self, actor: Actor, thread_id: str, *, message_id: str, text: str) -> dict:
        self.d.limiter.check("message.send", actor.customer_id, actor.user_id)
        self._check_id(thread_id)
        if not is_ulid(message_id):
            raise Invalid("A message id must be a ULID generated by the client.", code="invalid_message_id")
        body = clean_text(text, max_chars=self.d.settings.message_max_chars, field="message")
        head, _ = await self.access.require_member(actor, thread_id)
        cid = actor.customer_id

        seq = await self.d.store.find_message_seq(cid, thread_id, message_id)
        if seq is not None:
            existing = await self._message_at(cid, thread_id, seq, message_id)
            if existing is not None:
                return views.message(existing)      # a retry of a send that already landed
        else:
            head = await self._issue_seq(cid, thread_id, user_message=True, sender_id=actor.user_id, text=body)
            prior = await self.d.store.claim_message_id(cid, thread_id, message_id, head.seq)
            seq = head.seq if prior is None else prior
            if prior is not None:
                existing = await self._message_at(cid, thread_id, prior, message_id)
                if existing is not None:
                    return views.message(existing)

        msg = Message(id=message_id, threadId=thread_id, seq=seq, kind=MessageKind.USER,
                      senderId=actor.user_id, text=body, createdAt=self.d.clock.now())
        await self.d.store.append_message(cid, msg)
        await self._advance_read(cid, thread_id, actor.user_id, seq)
        head = await self.d.store.get_thread(cid, thread_id)
        members = await self.d.store.list_members(cid, thread_id)
        await self._fanout(head, members)
        view = views.message(msg)
        await self.d.realtime.publish(cid, [m.userId for m in members if m.active],
                                      {"type": "message.created", "threadId": thread_id, "message": view})
        await self.audit.record(actor.user_id, cid, "message.sent", thread_id=thread_id, message_id=message_id)
        return view

    async def post_system(self, customer_id: int, thread_id: str, *, event: str, text: str,
                          data: Optional[dict] = None) -> Message:
        """A note from the platform ("Priya added Ahmed"). It updates the
        conversation preview but does not make the conversation unread: the
        people it concerns are told through notifications instead."""
        head = await self._issue_seq(customer_id, thread_id, user_message=False, sender_id=None, text=text)
        msg = Message(id=new_ulid(), threadId=thread_id, seq=head.seq, kind=MessageKind.SYSTEM, text=text,
                      system={"event": event, **(data or {})}, createdAt=self.d.clock.now())
        await self.d.store.claim_message_id(customer_id, thread_id, msg.id, msg.seq)
        await self.d.store.append_message(customer_id, msg)
        members = await self.d.store.list_members(customer_id, thread_id)
        await self._fanout(head, members)
        await self.d.realtime.publish(customer_id, [m.userId for m in members if m.active],
                                      {"type": "message.created", "threadId": thread_id, "message": views.message(msg)})
        return msg

    async def mark_read(self, actor: Actor, thread_id: str, seq: int) -> dict:
        self._check_id(thread_id)
        head, _ = await self.access.require_member(actor, thread_id)
        seq = max(0, min(int(seq), head.seq))
        member = await self._advance_read(actor.customer_id, thread_id, actor.user_id, seq)
        if head.type != ThreadType.CHANNEL:
            members = await self.d.store.list_members(actor.customer_id, thread_id)
            others = [m.userId for m in members if m.active and m.userId != actor.user_id]
            if len(others) < self.d.settings.fanout_member_limit:
                await self.d.realtime.publish(actor.customer_id, others, {
                    "type": "thread.read", "threadId": thread_id, "userId": actor.user_id, "seq": member.lastReadSeq})
        return {"lastReadSeq": member.lastReadSeq}

    async def delete_message(self, actor: Actor, thread_id: str, message_id: str) -> dict:
        self._check_id(thread_id)
        head, _ = await self.access.require_member(actor, thread_id)
        seq = await self.d.store.find_message_seq(actor.customer_id, thread_id, message_id) if is_ulid(message_id) else None
        msg = await self._message_at(actor.customer_id, thread_id, seq, message_id) if seq else None
        if msg is None:
            raise NotFound("That message does not exist.")
        if msg.kind != MessageKind.USER or msg.senderId != actor.user_id:
            raise Forbidden("You can only delete your own messages.", code="not_your_message")
        if msg.deletedAt is None:
            now = self.d.clock.now()
            await self.d.store.put_tombstone(actor.customer_id, thread_id, message_id, actor.user_id, now)
            if head.seq == seq:
                def mutate(h: ThreadHead) -> ThreadHead:
                    if h.seq == seq:
                        h.lastPreview = "Message deleted"
                    return h
                head = await self.d.store.update_thread(actor.customer_id, thread_id, mutate)
                await self._fanout(head, await self.d.store.list_members(actor.customer_id, thread_id))
            members = await self.d.store.list_members(actor.customer_id, thread_id)
            await self.d.realtime.publish(actor.customer_id, [m.userId for m in members if m.active],
                                          {"type": "message.deleted", "threadId": thread_id, "messageId": message_id})
            await self.audit.record(actor.user_id, actor.customer_id, "message.deleted",
                                    thread_id=thread_id, message_id=message_id)
        return {"deleted": True}

    async def add_members(self, actor: Actor, thread_id: str, user_ids: list[int]) -> dict:
        self.d.limiter.check("thread.members", actor.customer_id, actor.user_id)
        self._check_id(thread_id)
        head, _ = await self.access.require_member(actor, thread_id)
        if head.type in (ThreadType.DIRECT, ThreadType.TASK):
            raise Invalid("People cannot be added to this kind of conversation.", code="invalid_type")
        cid = actor.customer_id
        current = {m.userId: m for m in await self.d.store.list_members(cid, thread_id)}
        new_ids = sorted({int(u) for u in user_ids if int(u) not in current or not current[int(u)].active})
        if not new_ids:
            return await self.get_thread(actor, thread_id)
        active_count = sum(1 for m in current.values() if m.active)
        if active_count + len(new_ids) > self.d.settings.group_max_members:
            raise Invalid(f"A conversation can have at most {self.d.settings.group_max_members} people.",
                          code="too_many_members")
        users = await self.access.require_colleagues(actor, new_ids)
        if head.anchor is not None:
            await self.access.require_anchor_access(cid, [actor.user_id, *new_ids], head.anchor, actor_id=actor.user_id)

        now = self.d.clock.now()
        for uid in new_ids:
            if uid in current:
                def rejoin(m: Member, now=now) -> Member:
                    m.leftAt = None
                    m.joinedAt = now
                    m.addedBy = actor.user_id
                    return m
                await self.d.store.update_member(cid, thread_id, uid, rejoin)
            else:
                await self.d.store.put_member(cid, Member(threadId=thread_id, userId=uid, joinedAt=now,
                                                          addedBy=actor.user_id))
        await self._recount(cid, thread_id)
        names = ", ".join(users[u].name for u in new_ids if u in users)
        # Visible to everyone, by decision: the people already here can see that
        # someone new can now read the whole history.
        await self.post_system(cid, thread_id, event="members_added",
                               text=f"{actor.user.name} added {names}", data={"userIds": new_ids, "by": actor.user_id})
        await self.notifier.notify(cid, new_ids, kind="added_to_conversation", actor_id=actor.user_id,
                                   title=f"{actor.user.name} added you to a conversation", thread_id=thread_id)
        await self.audit.record(actor.user_id, cid, "thread.members_added", thread_id=thread_id, targets=new_ids)
        return await self.get_thread(actor, thread_id)

    async def leave(self, actor: Actor, thread_id: str) -> dict:
        self._check_id(thread_id)
        head, _ = await self.access.require_member(actor, thread_id)
        if head.type in (ThreadType.DIRECT, ThreadType.TASK):
            raise Invalid("You cannot leave this kind of conversation.", code="invalid_type")
        cid = actor.customer_id
        now = self.d.clock.now()

        def depart(m: Member) -> Member:
            m.leftAt = m.leftAt or now
            return m
        await self.d.store.update_member(cid, thread_id, actor.user_id, depart)
        await self.d.store.update_inbox(cid, actor.user_id, thread_id, lambda _: None)
        await self._recount(cid, thread_id)
        await self.post_system(cid, thread_id, event="member_left", text=f"{actor.user.name} left",
                               data={"userId": actor.user_id})
        await self.audit.record(actor.user_id, cid, "thread.left", thread_id=thread_id)
        return {"left": True}

    async def set_muted(self, actor: Actor, thread_id: str, muted: bool) -> dict:
        self._check_id(thread_id)
        await self.access.require_member(actor, thread_id)

        def mutate(m: Member) -> Member:
            m.muted = bool(muted)
            return m
        member = await self.d.store.update_member(actor.customer_id, thread_id, actor.user_id, mutate)

        def entry_mutate(e: Optional[InboxEntry]) -> Optional[InboxEntry]:
            if e is not None:
                e.muted = member.muted
            return e
        await self.d.store.update_inbox(actor.customer_id, actor.user_id, thread_id, entry_mutate)
        return {"muted": member.muted}

    async def typing(self, actor: Actor, thread_id: str) -> dict:
        self.d.limiter.check("typing", actor.customer_id, actor.user_id)
        self._check_id(thread_id)
        head, _ = await self.access.require_member(actor, thread_id)
        if head.type != ThreadType.CHANNEL:
            members = await self.d.store.list_members(actor.customer_id, thread_id)
            await self.d.realtime.publish(actor.customer_id,
                                          [m.userId for m in members if m.active and m.userId != actor.user_id],
                                          {"type": "typing", "threadId": thread_id, "userId": actor.user_id})
        return {"ok": True}

    # ─────────────────────────────────────────────────────────── helpers ──
    @staticmethod
    def _check_id(thread_id: str) -> None:
        if not is_thread_id(thread_id):
            raise NotFound("That conversation does not exist.")

    async def _issue_seq(self, customer_id: int, thread_id: str, *, user_message: bool,
                         sender_id: Optional[int], text: str) -> ThreadHead:
        now = self.d.clock.now()
        short = preview(text)

        def mutate(h: ThreadHead) -> ThreadHead:
            h.seq += 1
            if user_message:
                h.lastUserSeq = h.seq
            h.lastMessageAt = now
            h.lastPreview = short
            h.lastSenderId = sender_id
            return h
        try:
            return await self.d.store.update_thread(customer_id, thread_id, mutate)
        except KeyError:
            raise NotFound("That conversation does not exist.")

    async def _advance_read(self, customer_id: int, thread_id: str, user_id: int, seq: int) -> Member:
        def mutate(m: Member) -> Member:
            m.lastReadSeq = max(m.lastReadSeq, seq)
            return m
        member = await self.d.store.update_member(customer_id, thread_id, user_id, mutate)

        def entry_mutate(e: Optional[InboxEntry]) -> Optional[InboxEntry]:
            if e is not None:
                e.lastReadSeq = max(e.lastReadSeq, member.lastReadSeq)
            return e
        await self.d.store.update_inbox(customer_id, user_id, thread_id, entry_mutate)
        return member

    async def _message_at(self, customer_id: int, thread_id: str, seq: int, message_id: str) -> Optional[Message]:
        for m in await self.d.store.list_messages(customer_id, thread_id, from_seq=seq, to_seq=seq):
            if m.id == message_id:
                return m
        return None

    async def _recount(self, customer_id: int, thread_id: str) -> ThreadHead:
        count = sum(1 for m in await self.d.store.list_members(customer_id, thread_id) if m.active)

        def mutate(h: ThreadHead) -> ThreadHead:
            h.memberCount = count
            return h
        return await self.d.store.update_thread(customer_id, thread_id, mutate)

    async def _fanout(self, head: ThreadHead, members: list[Member]) -> None:
        """Bring each member's inbox row up to date with the thread.

        Monotonic merge: a slower write carrying an older seq never rolls a
        row back, so concurrent sends settle on the newest state regardless of
        the order their fan-outs finish in."""
        active = [m for m in members if m.active]
        if head.type == ThreadType.CHANNEL:
            return   # channels are listed from membership, never fanned out
        names = [m.userId for m in active] if len(active) <= _SMALL_THREAD_NAMES else []
        gate = asyncio.Semaphore(_FANOUT_CONCURRENCY)

        async def one(m: Member) -> None:
            def mutate(entry: Optional[InboxEntry]) -> InboxEntry:
                e = entry or InboxEntry(threadId=head.id, type=head.type)
                if head.seq >= e.seq:
                    e.type = head.type
                    e.title = head.title
                    e.anchor = head.anchor
                    e.taskId = head.taskId
                    e.lastMessageAt = head.lastMessageAt or head.createdAt
                    e.lastPreview = head.lastPreview
                    e.lastSenderId = head.lastSenderId
                    e.seq = head.seq
                    e.memberIds = names
                e.lastUserSeq = max(e.lastUserSeq, head.lastUserSeq)
                e.lastReadSeq = max(e.lastReadSeq, m.lastReadSeq)
                e.muted = m.muted
                return e
            async with gate:
                try:
                    await self.d.store.update_inbox(head.customerId, m.userId, head.id, mutate)
                except Exception:
                    # Derived data: log and move on. The next message repairs it.
                    logger.exception("inbox fan-out failed thread=%s user=%s", head.id, m.userId)

        await asyncio.gather(*(one(m) for m in active))
