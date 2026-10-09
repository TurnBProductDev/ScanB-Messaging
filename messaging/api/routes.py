"""HTTP API, version 1.

Thin on purpose: parse, resolve the caller, call a service, return its dict.
Every rule lives in the services, where the tests exercise it directly.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from ..directory import DirectoryUnavailable
from ..errors import Forbidden, Invalid, Unavailable
from ..services.common import Actor
from .. import views

MAX_IDS = 200


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MessageIn(Body):
    id: str = Field(min_length=26, max_length=26)
    text: str = Field(max_length=20_000)


class StartThreadIn(Body):
    type: str
    memberIds: list[int] = Field(max_length=MAX_IDS)
    title: Optional[str] = Field(default=None, max_length=1000)
    context: Optional[str] = Field(default=None, max_length=64_000)
    message: Optional[MessageIn] = None


class MembersIn(Body):
    userIds: list[int] = Field(min_length=1, max_length=MAX_IDS)


class ReadIn(Body):
    seq: int = Field(ge=0)


class PrefsIn(Body):
    muted: bool


class NotificationsReadIn(Body):
    ids: Optional[list[str]] = Field(default=None, max_length=200)


class PeopleSearchIn(Body):
    query: str = Field(default="", max_length=100)
    context: Optional[str] = Field(default=None, max_length=64_000)
    threadId: Optional[str] = Field(default=None, max_length=40)
    limit: int = Field(default=20, ge=1, le=50)


class TaskIn(Body):
    title: str = Field(max_length=1000)
    description: str = Field(default="", max_length=20_000)
    ownerId: int
    watcherIds: list[int] = Field(default_factory=list, max_length=MAX_IDS)
    dueAt: Optional[datetime] = None
    priority: str = "normal"
    needsApproval: bool = False
    context: Optional[str] = Field(default=None, max_length=64_000)
    sourceThreadId: Optional[str] = Field(default=None, max_length=40)
    sourceMessageId: Optional[str] = Field(default=None, max_length=26)


class TaskStatusIn(Body):
    status: str
    note: Optional[str] = Field(default=None, max_length=5000)
    expectedVersion: int = Field(ge=1)


class TaskUpdateIn(Body):
    changes: dict[str, Any]
    expectedVersion: int = Field(ge=1)


class RequestUpdateIn(Body):
    note: Optional[str] = Field(default=None, max_length=2000)


class AnchorCountsIn(Body):
    keys: list[str] = Field(min_length=1, max_length=100)


def build_router(identity_dependency) -> APIRouter:
    router = APIRouter(prefix="/v1")

    async def actor(request: Request, identity=Depends(identity_dependency)) -> Actor:
        services = request.app.state.services
        try:
            user = await services.deps.directory.resolve_email(identity.email)
        except DirectoryUnavailable:
            raise Unavailable("Messaging is temporarily unavailable. Try again shortly.")
        if user is None or not user.usable:
            raise Forbidden("Messaging is not enabled for your account.", code="messaging_not_enabled")
        return Actor(user=user)

    def svc(request: Request):
        return request.app.state.services

    # ── me / activity ──────────────────────────────────────────────────
    @router.get("/me")
    async def me(request: Request, a: Actor = Depends(actor)):
        s = svc(request)
        return {
            "user": views.person(a.user, a.user_id),
            "customerId": a.customer_id,
            "permissions": {"teamTasks": a.user.canViewTeamTasks, "manageChannels": a.user.canManageChannels},
            "realtime": s.deps.realtime.mode,
        }

    @router.get("/activity")
    async def activity(request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.activity(a)

    @router.get("/notifications")
    async def notifications(request: Request, limit: int = Query(50, ge=1, le=100), a: Actor = Depends(actor)):
        return await svc(request).threads.notifications(a, limit=limit)

    @router.post("/notifications/read")
    async def notifications_read(body: NotificationsReadIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.mark_notifications_read(a, body.ids)

    @router.post("/realtime/negotiate")
    async def negotiate(request: Request, a: Actor = Depends(actor)):
        s = svc(request)
        url = await s.deps.realtime.client_url(a.customer_id, a.user_id)
        return {"mode": s.deps.realtime.mode if url else "poll", "url": url, "pollSeconds": 30}

    # ── people ─────────────────────────────────────────────────────────
    @router.post("/people/search")
    async def people_search(body: PeopleSearchIn, request: Request, a: Actor = Depends(actor)):
        s = svc(request)
        s.deps.limiter.check("people.search", a.customer_id, a.user_id)
        try:
            found = await s.deps.directory.search_users(a.customer_id, body.query, body.limit)
        except DirectoryUnavailable:
            raise Unavailable("People could not be searched right now.")
        found = [u for u in found if u.userId != a.user_id]
        anchor = await s.threads.anchor_for(a, context_token=body.context, thread_id=body.threadId)
        decisions = {}
        if anchor is not None and found:
            decisions = await s.access.anchor_decisions(a.customer_id, [u.userId for u in found], anchor)
        results = []
        for u in found:
            d = decisions.get(u.userId)
            results.append({**views.person(u, u.userId),
                            "eligible": d.allowed if d else True,
                            "reason": (d.reason if d and not d.allowed else None)})
        return {"people": results}

    # ── conversations ──────────────────────────────────────────────────
    @router.get("/threads")
    async def inbox(request: Request, cursor: Optional[str] = Query(None, max_length=200),
                    limit: int = Query(30, ge=1, le=100), filter: str = Query("all", max_length=20),
                    a: Actor = Depends(actor)):
        return await svc(request).threads.inbox(a, limit=limit, cursor=cursor, filter=filter)

    @router.post("/threads", status_code=201)
    async def start(body: StartThreadIn, request: Request, a: Actor = Depends(actor),
                    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key")):
        return await svc(request).threads.start(
            a, type=body.type, member_ids=body.memberIds, title=body.title, context_token=body.context,
            message_id=body.message.id if body.message else None,
            message_text=body.message.text if body.message else None, idempotency_key=idempotency_key)

    @router.get("/threads/{thread_id}")
    async def thread(thread_id: str, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.get_thread(a, thread_id)

    @router.get("/threads/{thread_id}/messages")
    async def messages(thread_id: str, request: Request, before: Optional[int] = Query(None, ge=1),
                       limit: int = Query(50, ge=1, le=100), a: Actor = Depends(actor)):
        return await svc(request).threads.list_messages(a, thread_id, before_seq=before, limit=limit)

    @router.post("/threads/{thread_id}/messages", status_code=201)
    async def send(thread_id: str, body: MessageIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.post_message(a, thread_id, message_id=body.id, text=body.text)

    @router.delete("/threads/{thread_id}/messages/{message_id}")
    async def delete(thread_id: str, message_id: str, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.delete_message(a, thread_id, message_id)

    @router.post("/threads/{thread_id}/read")
    async def read(thread_id: str, body: ReadIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.mark_read(a, thread_id, body.seq)

    @router.post("/threads/{thread_id}/members")
    async def add_members(thread_id: str, body: MembersIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.add_members(a, thread_id, body.userIds)

    @router.delete("/threads/{thread_id}/members/me")
    async def leave(thread_id: str, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.leave(a, thread_id)

    @router.put("/threads/{thread_id}/prefs")
    async def prefs(thread_id: str, body: PrefsIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.set_muted(a, thread_id, body.muted)

    @router.post("/threads/{thread_id}/typing")
    async def typing(thread_id: str, request: Request, a: Actor = Depends(actor)):
        return await svc(request).threads.typing(a, thread_id)

    # ── tasks ──────────────────────────────────────────────────────────
    @router.get("/tasks")
    async def tasks(request: Request, view: str = Query("assigned_to_me", max_length=20),
                    status: Optional[str] = Query(None, max_length=100), due: Optional[str] = Query(None, max_length=20),
                    priority: Optional[str] = Query(None, max_length=50), source: Optional[str] = Query(None, max_length=20),
                    person: Optional[int] = None, q: Optional[str] = Query(None, max_length=100),
                    a: Actor = Depends(actor)):
        return await svc(request).tasks.list(a, view=view, status=status, due=due, priority=priority,
                                             source=source, person=person, q=q)

    @router.post("/tasks", status_code=201)
    async def create_task(body: TaskIn, request: Request, a: Actor = Depends(actor),
                          idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key")):
        return await svc(request).tasks.create(
            a, title=body.title, description=body.description, owner_id=body.ownerId,
            watcher_ids=body.watcherIds, due_at=body.dueAt, priority=body.priority,
            needs_approval=body.needsApproval, context_token=body.context,
            source_thread_id=body.sourceThreadId, source_message_id=body.sourceMessageId,
            idempotency_key=idempotency_key)

    @router.post("/tasks/anchor-counts")
    async def anchor_counts(body: AnchorCountsIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).tasks.counts_by_anchor(a, body.keys)

    @router.get("/tasks/{task_id}")
    async def task(task_id: str, request: Request, a: Actor = Depends(actor)):
        return await svc(request).tasks.get(a, task_id)

    @router.post("/tasks/{task_id}/status")
    async def task_status(task_id: str, body: TaskStatusIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).tasks.change_status(a, task_id, to=body.status, note=body.note,
                                                      expected_version=body.expectedVersion)

    @router.patch("/tasks/{task_id}")
    async def task_update(task_id: str, body: TaskUpdateIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).tasks.update(a, task_id, changes=body.changes,
                                               expected_version=body.expectedVersion)

    @router.post("/tasks/{task_id}/request-update")
    async def task_request_update(task_id: str, body: RequestUpdateIn, request: Request, a: Actor = Depends(actor)):
        return await svc(request).tasks.request_update(a, task_id, note=body.note)

    return router
