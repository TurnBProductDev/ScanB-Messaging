"""One error shape for every failure: {"code", "message", ...extra}.

Codes are stable and meant for the caller to branch on; messages are written
for the person reading them.
"""
from __future__ import annotations


class AppError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message: str, *, code: str | None = None, status: int | None = None, **extra):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        if status:
            self.status = status
        self.extra = extra

    def body(self) -> dict:
        return {"code": self.code, "message": self.message, **self.extra}


class NotFound(AppError):
    status = 404
    code = "not_found"


class Forbidden(AppError):
    status = 403
    code = "forbidden"


class Invalid(AppError):
    status = 422
    code = "invalid_request"


class Conflict(AppError):
    status = 409
    code = "conflict"


class RateLimited(AppError):
    status = 429
    code = "rate_limited"


class Unavailable(AppError):
    status = 503
    code = "unavailable"
