"""Verified context: the item a conversation or task is about.

The Web App fetches the real item (an insight from /kpi/insights, a summary
from /report/summary), builds the anchor and snapshot, and signs them. This
service accepts context ONLY with a valid signature. A browser cannot mint one,
so a fabricated "AI insight" with invented numbers cannot be sent under the
platform's name.

Token format (compact, JWS-like, no header):

    base64url(payload_json_utf8) "." base64url(HMAC-SHA256(key, first_part_ascii))

The MAC covers the encoded bytes exactly as sent, so neither side has to agree
on JSON canonicalisation. The payload is bound to one customer and one user,
and expires, so a token lifted from one request is useless to anyone else and
useless later.

The Web App side is Service/Messaging/MessagingContextSigner.cs; the two must change
together.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from .errors import Invalid
from .models import Anchor

MAX_TOKEN_CHARS = 64_000
MAX_SNAPSHOT_BYTES = 32_000
CLOCK_SKEW_SECONDS = 60


@dataclass(frozen=True)
class VerifiedContext:
    anchor: Anchor
    snapshot: dict[str, Any]
    issued_at: datetime


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64d(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def sign(payload: dict[str, Any], key: bytes) -> str:
    """Used by tests and by local tooling; production tokens come from the Web App."""
    body = _b64e(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    mac = hmac.new(key, body.encode("ascii"), hashlib.sha256).digest()
    return f"{body}.{_b64e(mac)}"


def verify(token: str, keys: list[bytes], *, now: datetime, max_age_seconds: int,
           customer_id: int, user_id: int) -> VerifiedContext:
    def reject(reason: str) -> Invalid:
        return Invalid("The attached item could not be verified. Reopen it and try again.",
                       code="context_invalid", reason=reason)

    if not token or len(token) > MAX_TOKEN_CHARS or token.count(".") != 1:
        raise reject("malformed")
    body, mac_text = token.split(".")
    try:
        mac = _b64d(mac_text)
    except Exception:
        raise reject("malformed")
    if not any(hmac.compare_digest(hmac.new(k, body.encode("ascii"), hashlib.sha256).digest(), mac) for k in keys):
        raise reject("signature")
    try:
        payload = json.loads(_b64d(body).decode("utf-8"))
    except Exception:
        raise reject("malformed")
    if not isinstance(payload, dict) or payload.get("v") != 1:
        raise reject("version")

    if payload.get("customerId") != customer_id or payload.get("userId") != user_id:
        raise reject("audience")

    iat, exp = payload.get("iat"), payload.get("exp")
    if not isinstance(iat, int) or not isinstance(exp, int):
        raise reject("expiry")
    ts = int(now.timestamp())
    if exp - iat > max_age_seconds or iat > ts + CLOCK_SKEW_SECONDS or exp < ts - CLOCK_SKEW_SECONDS:
        raise reject("expiry")

    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, dict):
        raise reject("snapshot")
    if len(json.dumps(snapshot, separators=(",", ":")).encode("utf-8")) > MAX_SNAPSHOT_BYTES:
        raise reject("snapshot_too_large")
    try:
        anchor = Anchor.model_validate(payload.get("anchor"))
    except ValidationError:
        raise reject("anchor")

    return VerifiedContext(anchor=anchor, snapshot=snapshot,
                           issued_at=datetime.fromtimestamp(iat, tz=timezone.utc))
