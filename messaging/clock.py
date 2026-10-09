from __future__ import annotations

from datetime import datetime, timezone


class Clock:
    """Injected everywhere time matters, so reminders and expiry are testable."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FixedClock(Clock):
    def __init__(self, at: datetime):
        self.at = at

    def now(self) -> datetime:
        return self.at

    def advance(self, **delta) -> None:
        from datetime import timedelta
        self.at = self.at + timedelta(**delta)
