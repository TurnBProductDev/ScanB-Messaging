"""Who people are and what they may see.

The Web App owns users, roles and report entitlements in its SQL database, and
it stays the only source of truth for them. This service never copies that
model; it asks, through the Web App's internal API, and caches the answer
briefly.

Failure is closed. If the directory cannot answer, nobody is resolved and no
item is visible: a messaging outage is recoverable, a data leak is not.

Web App side: Service/Messaging/MessagingInternalApi.cs. The endpoint shapes below
and that file must change together.

    GET  /internal/messaging/v1/users/by-email?email=
    POST /internal/messaging/v1/users/lookup          {customerId, userIds}
    GET  /internal/messaging/v1/customers/{id}/users?q=&limit=
    POST /internal/messaging/v1/access/anchor         {customerId, userIds, anchor}
"""
from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import httpx
from pydantic import BaseModel, ConfigDict

from .models import Anchor, AnchorKind, AnchorResolution

logger = logging.getLogger("messaging.directory")


class DirectoryUser(BaseModel):
    model_config = ConfigDict(extra="ignore")
    userId: int
    customerId: int
    name: str
    role: Optional[str] = None
    active: bool = True
    customerActive: bool = True
    messagingEnabled: bool = True
    isCustomerAdmin: bool = False
    canViewTeamTasks: bool = False
    canManageChannels: bool = False

    @property
    def usable(self) -> bool:
        return self.active and self.customerActive and self.messagingEnabled


class AccessDecision(BaseModel):
    allowed: bool
    reason: Optional[str] = None


class DirectoryUnavailable(Exception):
    pass


class Directory(ABC):
    @abstractmethod
    async def resolve_email(self, email: str) -> Optional[DirectoryUser]: ...

    @abstractmethod
    async def get_users(self, customer_id: int, user_ids: list[int]) -> dict[int, DirectoryUser]:
        """Includes inactive users, so history stays attributable."""

    @abstractmethod
    async def search_users(self, customer_id: int, query: str, limit: int) -> list[DirectoryUser]:
        """Active users only."""

    @abstractmethod
    async def check_anchor(self, customer_id: int, user_ids: list[int], anchor: Anchor) -> dict[int, AccessDecision]: ...

    async def close(self) -> None:
        return None


# ───────────────────────────────────────────────────────────────── static ──
class StaticDirectory(Directory):
    """Local development and tests. Reads dev/directory.dev.json (sample people)."""

    def __init__(self, data: dict) -> None:
        self._users = {u["userId"]: DirectoryUser.model_validate(u) for u in data.get("users", [])}
        self._emails = {u["email"].lower(): u["userId"] for u in data.get("users", []) if u.get("email")}
        self._grants = {int(k): set(v) for k, v in data.get("reportGrants", {}).items()}
        self._module_of = {int(k): v for k, v in data.get("moduleOfReport", {}).items()}
        self._pb_of = {int(k): v for k, v in data.get("pbReportIdOfReport", {}).items()}
        self._ai = {int(k): v for k, v in data.get("aiAccess", {}).items()}

    @classmethod
    def from_file(cls, path: str) -> "StaticDirectory":
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    async def resolve_email(self, email):
        uid = self._emails.get((email or "").lower())
        return self._users.get(uid) if uid else None

    async def get_users(self, customer_id, user_ids):
        return {u: self._users[u] for u in user_ids if u in self._users and self._users[u].customerId == customer_id}

    async def search_users(self, customer_id, query, limit):
        q = (query or "").strip().lower()
        hits = [u for u in self._users.values()
                if u.customerId == customer_id and u.usable
                and (not q or q in u.name.lower() or q in (u.role or "").lower())]
        return sorted(hits, key=lambda u: u.name.lower())[:limit]

    async def check_anchor(self, customer_id, user_ids, anchor):
        result = {}
        for uid in user_ids:
            user = self._users.get(uid)
            if user is None or user.customerId != customer_id or not user.usable:
                result[uid] = AccessDecision(allowed=False, reason="not_a_colleague")
                continue
            grants = self._grants.get(uid, set())
            if anchor.kind == AnchorKind.CHAT_ANSWER:
                ok = self._ai.get(uid, False)
            elif anchor.resolution == AnchorResolution.TENANT_ONLY:
                ok = True
            elif anchor.resolution == AnchorResolution.REPORT and anchor.reportRowId is not None:
                ok = anchor.reportRowId in grants
            elif anchor.resolution == AnchorResolution.REPORT and anchor.pbReportId:
                ok = any(self._pb_of.get(r) == anchor.pbReportId for r in grants)
            elif anchor.resolution == AnchorResolution.MODULE and anchor.moduleId is not None:
                ok = any(self._module_of.get(r) == anchor.moduleId for r in grants)
            else:
                ok = False
            result[uid] = AccessDecision(allowed=ok, reason=None if ok else "no_report_access")
        return result


# ───────────────────────────────────────────────────────────────── web app ──
class WebAppDirectory(Directory):
    """Calls the Web App with this service's own app token (client credentials
    or managed identity) for the scope the Web App exposes. A person's token is
    never forwarded: the Web App decides what a user may see, it does not take
    the user's word for it."""

    def __init__(self, *, base_url: str, scope: str = "", dev_key: str = "", timeout_seconds: float = 5.0) -> None:
        # The dev key only exists on a developer's laptop (settings refuse it in
        # production), and there the Web App runs behind the ASP.NET self-signed
        # localhost certificate, which no trust store accepts. Certificates are
        # verified everywhere else, which is every deployment.
        self._http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout_seconds,
                                       verify=not bool(dev_key))
        self._scope = scope
        self._dev_key = dev_key
        self._credential = None
        self._token: tuple[str, float] | None = None

    async def _headers(self) -> dict[str, str]:
        if self._dev_key:
            return {"X-Internal-Dev-Key": self._dev_key}
        if self._token is None or self._token[1] - 120 < time.time():
            if self._credential is None:
                from azure.identity.aio import DefaultAzureCredential
                self._credential = DefaultAzureCredential()
            token = await self._credential.get_token(self._scope)
            self._token = (token.token, float(token.expires_on))
        return {"Authorization": f"Bearer {self._token[0]}"}

    async def _call(self, method: str, path: str, **kwargs) -> Optional[dict]:
        try:
            response = await self._http.request(method, path, headers=await self._headers(), **kwargs)
        except Exception as exc:
            logger.error("directory %s %s failed: %s", method, path, type(exc).__name__)
            raise DirectoryUnavailable(str(exc)) from exc
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            logger.error("directory %s %s returned %s", method, path, response.status_code)
            raise DirectoryUnavailable(f"status {response.status_code}")
        return response.json()

    async def resolve_email(self, email):
        body = await self._call("GET", "/internal/messaging/v1/users/by-email", params={"email": email})
        return DirectoryUser.model_validate(body) if body else None

    async def get_users(self, customer_id, user_ids):
        if not user_ids:
            return {}
        body = await self._call("POST", "/internal/messaging/v1/users/lookup",
                                json={"customerId": customer_id, "userIds": sorted(set(user_ids))})
        users = [DirectoryUser.model_validate(u) for u in (body or {}).get("users", [])]
        return {u.userId: u for u in users if u.customerId == customer_id}

    async def search_users(self, customer_id, query, limit):
        body = await self._call("GET", f"/internal/messaging/v1/customers/{customer_id}/users",
                                params={"q": query or "", "limit": limit})
        users = [DirectoryUser.model_validate(u) for u in (body or {}).get("users", [])]
        return [u for u in users if u.customerId == customer_id]

    async def check_anchor(self, customer_id, user_ids, anchor):
        if not user_ids:
            return {}
        body = await self._call("POST", "/internal/messaging/v1/access/anchor", json={
            "customerId": customer_id,
            "userIds": sorted(set(user_ids)),
            "anchor": anchor.model_dump(include={"kind", "resolution", "reportRowId", "moduleId", "pbReportId", "dashboardKey"}),
        })
        decisions = {d["userId"]: AccessDecision(allowed=bool(d.get("allowed")), reason=d.get("reason"))
                     for d in (body or {}).get("decisions", [])}
        # Anyone the Web App did not answer for is denied, not assumed.
        return {uid: decisions.get(uid, AccessDecision(allowed=False, reason="no_answer")) for uid in user_ids}

    async def close(self):
        await self._http.aclose()
        if self._credential is not None:
            await self._credential.close()


# ───────────────────────────────────────────────────────────────── caching ──
class CachedDirectory(Directory):
    """Short TTL cache. Permission changes take effect within `ttl_seconds`;
    that window is the documented cost of not calling the Web App on every
    message. Failures are never cached."""

    def __init__(self, inner: Directory, ttl_seconds: int = 60, clock=time.monotonic) -> None:
        self._inner = inner
        self._ttl = ttl_seconds
        self._clock = clock
        self._emails: dict[str, tuple[float, Optional[DirectoryUser]]] = {}
        self._users: dict[tuple[int, int], tuple[float, DirectoryUser]] = {}
        self._anchors: dict[tuple, tuple[float, AccessDecision]] = {}

    def _fresh(self, stamp: float) -> bool:
        return self._clock() - stamp < self._ttl

    def _trim(self) -> None:
        for cache in (self._emails, self._users, self._anchors):
            if len(cache) > 20_000:
                cache.clear()

    async def resolve_email(self, email):
        key = (email or "").lower()
        hit = self._emails.get(key)
        if hit and self._fresh(hit[0]):
            return hit[1]
        user = await self._inner.resolve_email(email)
        self._trim()
        self._emails[key] = (self._clock(), user)
        return user

    async def get_users(self, customer_id, user_ids):
        found, missing = {}, []
        for uid in set(user_ids):
            hit = self._users.get((customer_id, uid))
            if hit and self._fresh(hit[0]):
                found[uid] = hit[1]
            else:
                missing.append(uid)
        if missing:
            fetched = await self._inner.get_users(customer_id, missing)
            self._trim()
            for uid, user in fetched.items():
                self._users[(customer_id, uid)] = (self._clock(), user)
                found[uid] = user
        return found

    async def search_users(self, customer_id, query, limit):
        return await self._inner.search_users(customer_id, query, limit)

    async def check_anchor(self, customer_id, user_ids, anchor):
        anchor_key = (anchor.kind, anchor.resolution, anchor.reportRowId, anchor.moduleId, anchor.pbReportId, anchor.dashboardKey)
        found, missing = {}, []
        for uid in set(user_ids):
            hit = self._anchors.get((customer_id, uid, anchor_key))
            if hit and self._fresh(hit[0]):
                found[uid] = hit[1]
            else:
                missing.append(uid)
        if missing:
            fetched = await self._inner.check_anchor(customer_id, missing, anchor)
            self._trim()
            for uid, decision in fetched.items():
                self._anchors[(customer_id, uid, anchor_key)] = (self._clock(), decision)
                found[uid] = decision
        return found

    async def close(self):
        await self._inner.close()
