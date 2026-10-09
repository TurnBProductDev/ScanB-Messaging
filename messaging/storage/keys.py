"""Names and keys shared by both stores, so ordering and cursors agree."""
from __future__ import annotations

from datetime import datetime

_MAX_MS = 9_999_999_999_999


def container_name(customer_id: int) -> str:
    """One blob container per customer: the platform-wide tenancy rule.

    Separate from the AI content container on purpose: not every customer has
    ScanB AI, and messaging must not depend on it being provisioned."""
    if customer_id <= 0:
        raise ValueError("customer id must be positive")
    return f"msg-c{customer_id}"


def table_name(customer_id: int) -> str:
    if customer_id <= 0:
        raise ValueError("customer id must be positive")
    return f"MsgC{customer_id}"


TENANTS_TABLE = "MsgTenants"


def inverted_ms(at: datetime) -> str:
    """Newest-first ordering for keys that sort ascending."""
    return f"{_MAX_MS - int(at.timestamp() * 1000):013d}"


def inbox_order_key(last_activity: datetime, thread_id: str) -> str:
    return f"o_{inverted_ms(last_activity)}_{thread_id}"


_ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ULID_INVERT = str.maketrans(_ULID_ALPHABET, _ULID_ALPHABET[::-1])


def notification_key(created_at: datetime, notification_id: str) -> str:
    # Both halves inverted: two notifications in the same millisecond must
    # still list newest first, and ULIDs sort oldest first on their own.
    return f"{inverted_ms(created_at)}_{notification_id.translate(_ULID_INVERT)}"


def segment_of(seq: int, segment_size: int) -> int:
    return (seq - 1) // segment_size


def segment_blob(thread_id: str, segment: int) -> str:
    return f"threads/{thread_id}/messages/{segment:06d}.jsonl"


def context_blob(thread_id: str) -> str:
    return f"threads/{thread_id}/context.json"


def task_events_blob(task_id: str) -> str:
    return f"tasks/{task_id}/events.jsonl"


def audit_blob(at: datetime) -> str:
    return f"audit/{at:%Y/%m/%d/%H}.jsonl"


def audit_day_prefix(day) -> str:
    return f"audit/{day:%Y/%m/%d}/"
