"""What a person typed, made safe to store and show.

Messages are plain text end to end. Nothing here escapes HTML, on purpose:
escaping belongs to whoever renders, and every renderer in the platform sets
text with textContent. Storing pre-escaped text would double-escape there and
tempt someone to render it with innerHTML elsewhere.
"""
from __future__ import annotations

import re
import unicodedata

from .errors import Invalid

# C0/C1 controls except tab and newline, plus bidi overrides that can make a
# message display differently from what it says (the "Trojan Source" trick).
_STRIP_RANGES = ((0x00, 0x08), (0x0B, 0x1F), (0x7F, 0x9F), (0x202A, 0x202E), (0x2066, 0x2069))
_STRIP = re.compile("[" + "".join(f"{re.escape(chr(a))}-{re.escape(chr(b))}" for a, b in _STRIP_RANGES) + "]")
_BLANK_RUNS = re.compile(r"\n{4,}")


def clean_text(value: str | None, *, max_chars: int, field: str = "text", allow_empty: bool = False) -> str:
    text = unicodedata.normalize("NFC", value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _STRIP.sub("", text)
    text = _BLANK_RUNS.sub("\n\n\n", text)
    text = "\n".join(line.rstrip() for line in text.split("\n")).strip()
    if not text and not allow_empty:
        raise Invalid(f"The {field} is empty.", code="empty_text")
    if len(text) > max_chars:
        raise Invalid(f"The {field} is longer than {max_chars} characters.", code="text_too_long")
    return text


def preview(text: str, limit: int = 140) -> str:
    single = " ".join(text.split())
    return single if len(single) <= limit else single[: limit - 1].rstrip() + "…"
