"""Identifiers.

ULIDs: 26 characters, sortable by creation time, generated without a round
trip. Clients generate message ids themselves so a retried send is recognised
as the same message rather than stored twice.
"""
from __future__ import annotations

import os
import re
import threading
import time

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
_lock = threading.Lock()
_last = [0, 0]   # [timestamp ms, random part] of the previous id


def new_ulid(ts_ms: int | None = None) -> str:
    """Monotonic within this process: ids made in the same millisecond still
    sort in the order they were made (the ULID spec's monotonic mode)."""
    ts = int(time.time() * 1000) if ts_ms is None else ts_ms
    with _lock:
        if ts <= _last[0]:
            ts = _last[0]
            rand = (_last[1] + 1) & ((1 << 80) - 1)
        else:
            rand = int.from_bytes(os.urandom(10), "big")
        _last[0], _last[1] = ts, rand
    value = (ts << 80) | rand
    chars = []
    for _ in range(26):
        chars.append(_ALPHABET[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def is_ulid(value: str) -> bool:
    return bool(value) and bool(_ULID_RE.match(value))


def direct_thread_id(user_a: int, user_b: int) -> str:
    """One direct conversation per pair of people, whoever starts it."""
    low, high = sorted((user_a, user_b))
    return f"dm-{low}-{high}"


_THREAD_ID_RE = re.compile(r"^(dm-\d+-\d+|[0-9A-HJKMNP-TV-Z]{26})$")


def is_thread_id(value: str) -> bool:
    return bool(value) and bool(_THREAD_ID_RE.match(value))
