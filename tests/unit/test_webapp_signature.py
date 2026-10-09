"""The Web App signs context; this service verifies it. This test is the seam.

TOKEN below was produced by the PRODUCTION C# signer
(TurnB_ScanB_WebApplication/Service/Messaging/MessagingContextSigner.cs)
compiled and run as-is, with a fixed key, a fixed time and the payload below.

If either side changes how it builds or reads a token, this fails. Nothing
else would catch it before Ask Team stopped working in the app.

To regenerate after a DELIBERATE change, run the signer with:
    key   base64 of 32 "k" characters
    now   1789000000
    anchor/snapshot as asserted below
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from messaging.context import verify
from messaging.errors import Invalid

pytestmark = pytest.mark.unit

TOKEN = (
    "eyJ2IjoxLCJjdXN0b21lcklkIjoxMiwidXNlcklkIjoxMDEsImlhdCI6MTc4OTAwMDAwMCwiZXhwIjoxNzg5MDAwMzAwLCJh"
    "bmNob3IiOnsia2luZCI6Imluc2lnaHQiLCJyZXNvbHV0aW9uIjoicmVwb3J0Iiwia2V5IjoiaW5zaWdodDp0YXJnZXRfdHJh"
    "Y2tlcjo0OjIwMjYtMDktMTUiLCJ0aXRsZSI6IlNUMiBzYWxlcyAxOCUgdW5kZXIgdGFyZ2V0IiwicmVwb3J0Um93SWQiOjEx"
    "LCJtb2R1bGVJZCI6MywidXJsIjoiL1Bvd2VyQmkvRW1iZWRSZXBvcnQvMTEifSwic25hcHNob3QiOnsibWV0cmljIjoiTmV0"
    "IHNhbGVzIiwidmFsdWUiOiJVU0QgNTUuM0siLCJkZWx0YSI6Ii0xOCUiLCJkZWx0YURpcmVjdGlvbiI6ImRvd24iLCJyYXdW"
    "YWx1ZSI6NTUzMDAsInNvdXJjZSI6ImxpdmUifX0"
    ".t_Jc142nT1GL2d3hHtZWEKwvl54ZNQYtU-glrkLPvLM"
)

KEY = b"k" * 32
SIGNED_AT = datetime.fromtimestamp(1789000000, tz=timezone.utc)


def check(token=TOKEN, *, key=KEY, customer_id=12, user_id=101, now=SIGNED_AT):
    return verify(token, [key], now=now, max_age_seconds=300, customer_id=customer_id, user_id=user_id)


def test_web_app_token_verifies():
    context = check()
    assert context.anchor.kind == "insight"
    assert context.anchor.resolution == "report"
    assert context.anchor.key == "insight:target_tracker:4:2026-09-15"
    assert context.anchor.title == "ST2 sales 18% under target"
    assert context.anchor.reportRowId == 11 and context.anchor.moduleId == 3
    assert context.anchor.url == "/PowerBi/EmbedReport/11"
    # camelCase, nulls omitted: the C# serializer settings this side relies on
    assert context.anchor.pbReportId is None and context.anchor.dashboardKey is None
    assert context.snapshot["value"] == "USD 55.3K"
    assert context.snapshot["rawValue"] == 55300


def test_web_app_token_is_bound_to_one_person_and_customer():
    for kwargs in ({"user_id": 102}, {"customer_id": 13}):
        with pytest.raises(Invalid) as err:
            check(**kwargs)
        assert err.value.extra["reason"] == "audience"


def test_web_app_token_expires():
    from datetime import timedelta
    with pytest.raises(Invalid) as err:
        check(now=SIGNED_AT + timedelta(minutes=10))
    assert err.value.extra["reason"] == "expiry"


def test_web_app_token_cannot_be_edited():
    """Restate the figure and the token stops verifying: what colleagues see is
    what the Web App fetched, not what a browser claimed."""
    import base64
    import json

    body, mac = TOKEN.split(".")
    payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    assert payload["snapshot"]["value"] == "USD 55.3K"
    payload["snapshot"]["value"] = "USD 99.9K"
    edited = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")).rstrip(b"=").decode("ascii")

    with pytest.raises(Invalid) as err:
        check(edited + "." + mac)
    assert err.value.extra["reason"] == "signature"


def test_another_key_does_not_verify():
    with pytest.raises(Invalid) as err:
        check(key=b"z" * 32)
    assert err.value.extra["reason"] == "signature"
