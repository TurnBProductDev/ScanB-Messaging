"""What every service needs, wired once."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, Optional

from ..clock import Clock
from ..config import Settings
from ..directory import AccessDecision, Directory, DirectoryUnavailable, DirectoryUser
from ..errors import Forbidden, Invalid, NotFound, Unavailable
from ..ids import is_thread_id, new_ulid
from ..models import (
    Anchor, AnchorResolution, AuditEvent, Member, Notification, ThreadHead,
)
from ..ratelimit import RateLimiter
from ..realtime import RealtimePublisher
from ..storage.base import Store

logger = logging.getLogger("messaging.services")


@dataclass
class Actor:
    """The signed-in person, as the directory knows them. Every service call
    takes one; nothing downstream accepts a user id from request input."""
    user: DirectoryUser

    @property
    def user_id(self) -> int:
        return self.user.userId

    @property
    def customer_id(self) -> int:
        return self.user.customerId


@dataclass
class Deps:
    settings: Settings
    store: Store
    directory: Directory
    realtime: RealtimePublisher
    clock: Clock
    limiter: RateLimiter = field(default_factory=RateLimiter)


class Access:
    """The access rules, in one place.

    Two gates:
      * membership: a person sees a conversation only while they are in it.
        Anyone else gets 404, so a conversation they were never part of is
        indistinguishable from one that does not exist.
      * the attached item: checked when people are added AND every time it is
        shown, because permissions change after a message is sent.
    """

    def __init__(self, deps: Deps) -> None:
        self.d = deps

    async def require_member(self, actor: Actor, thread_id: str) -> tuple[ThreadHead, Member]:
        # Validated here, at the one gate every path goes through, before the
        # id can become part of a storage key.
        if not is_thread_id(thread_id or ""):
            raise NotFound("That conversation does not exist.")
        head = await self.d.store.get_thread(actor.customer_id, thread_id)
        # customerId is compared even though the store is already partitioned by
        # customer. The fence is checked, not assumed.
        if head is None or head.customerId != actor.customer_id:
            raise NotFound("That conversation does not exist.")
        member = await self.d.store.get_member(actor.customer_id, thread_id, actor.user_id)
        if member is None or not member.active:
            raise NotFound("That conversation does not exist.")
        return head, member

    async def people(self, customer_id: int, user_ids: Iterable[int]) -> dict[int, DirectoryUser]:
        ids = sorted({u for u in user_ids if u is not None})
        if not ids:
            return {}
        try:
            return await self.d.directory.get_users(customer_id, ids)
        except DirectoryUnavailable:
            # Names are cosmetic. A missing name renders as "Unknown person";
            # it must not take the conversation down.
            logger.warning("directory unavailable while resolving names")
            return {}

    async def require_colleagues(self, actor: Actor, user_ids: Iterable[int]) -> dict[int, DirectoryUser]:
        ids = sorted({u for u in user_ids if u != actor.user_id})
        if not ids:
            return {}
        try:
            users = await self.d.directory.get_users(actor.customer_id, ids)
        except DirectoryUnavailable:
            raise Unavailable("People could not be checked right now. Try again shortly.")
        problems = []
        for uid in ids:
            user = users.get(uid)
            if user is None or user.customerId != actor.customer_id:
                problems.append({"userId": uid, "reason": "not_a_colleague"})
            elif not user.usable:
                problems.append({"userId": uid, "reason": "inactive"})
        if problems:
            raise Invalid("Some of the people chosen cannot be added.", code="ineligible_people", people=problems)
        return users

    async def anchor_decisions(self, customer_id: int, user_ids: Iterable[int], anchor: Anchor) -> dict[int, AccessDecision]:
        """Raises Unavailable if the directory cannot answer."""
        ids = sorted(set(user_ids))
        try:
            if anchor.resolution == AnchorResolution.TENANT_ONLY and anchor.kind != "chat_answer":
                users = await self.d.directory.get_users(customer_id, ids)
                return {uid: AccessDecision(allowed=bool(users.get(uid) and users[uid].usable),
                                            reason=None if users.get(uid) and users[uid].usable else "not_a_colleague")
                        for uid in ids}
            return await self.d.directory.check_anchor(customer_id, ids, anchor)
        except DirectoryUnavailable:
            raise Unavailable("Access to the attached item could not be checked right now. Try again shortly.")

    async def require_anchor_access(self, customer_id: int, user_ids: Iterable[int], anchor: Anchor,
                                    *, actor_id: int) -> None:
        decisions = await self.anchor_decisions(customer_id, user_ids, anchor)
        if not decisions.get(actor_id, AccessDecision(allowed=False)).allowed:
            raise Forbidden("You no longer have access to the attached item.", code="context_forbidden")
        denied = [{"userId": uid, "reason": d.reason or "no_report_access"}
                  for uid, d in decisions.items() if uid != actor_id and not d.allowed]
        if denied:
            raise Invalid("Some of the people chosen cannot see the attached item.",
                          code="ineligible_people", people=denied)

    async def can_see_anchor(self, actor: Actor, anchor: Anchor) -> bool:
        """For showing an item. Fails closed: unknown means hidden."""
        try:
            decisions = await self.anchor_decisions(actor.customer_id, [actor.user_id], anchor)
        except Unavailable:
            return False
        decision = decisions.get(actor.user_id)
        return bool(decision and decision.allowed)


class Auditor:
    def __init__(self, deps: Deps) -> None:
        self.d = deps

    async def record(self, actor_id: Optional[int], customer_id: int, action: str, *,
                     thread_id: str | None = None, message_id: str | None = None, task_id: str | None = None,
                     targets: Iterable[int] = (), **detail) -> None:
        event = AuditEvent(id=new_ulid(), at=self.d.clock.now(), customerId=customer_id, actorId=actor_id,
                           action=action, threadId=thread_id, messageId=message_id, taskId=task_id,
                           targetUserIds=sorted(set(targets)), detail=detail)
        try:
            await self.d.store.append_audit(event)
        except Exception:
            # Audit write failures are loud, not silent. They do not fail the
            # user's action because the action is already committed; the log
            # line is what alerting keys on.
            logger.exception("AUDIT WRITE FAILED action=%s customer=%s", action, customer_id)


class Notifier:
    def __init__(self, deps: Deps) -> None:
        self.d = deps

    async def notify(self, customer_id: int, user_ids: Iterable[int], *, kind: str, title: str, body: str = "",
                     task_id: str | None = None, thread_id: str | None = None, actor_id: int | None = None) -> None:
        now = self.d.clock.now()
        targets = sorted({u for u in user_ids if u is not None and u != actor_id})
        for uid in targets:
            note = Notification(id=new_ulid(), userId=uid, kind=kind, title=title[:200], body=body[:500],
                                taskId=task_id, threadId=thread_id, actorId=actor_id, createdAt=now)
            await self.d.store.add_notification(customer_id, note)
            await self.d.realtime.publish(customer_id, [uid], {
                "type": "notification.created",
                "notification": note.model_dump(mode="json"),
            })
