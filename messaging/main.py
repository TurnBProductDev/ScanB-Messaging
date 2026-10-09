"""Application factory.

    uvicorn messaging.main:app            (reads MSG_* settings)
    python -m messaging.worker            (task reminders)
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .api.routes import build_router
from .auth import build_identity_dependency
from .clock import Clock
from .config import Settings, get_settings
from .directory import CachedDirectory, Directory, StaticDirectory, WebAppDirectory
from .errors import AppError
from .ratelimit import RateLimiter
from .realtime import RealtimePublisher, WebPubSubPublisher
from .services.common import Access, Auditor, Deps, Notifier
from .services.reminders import ReminderService
from .services.tasks import TaskService
from .services.threads import ThreadService
from .storage.base import ConcurrencyConflict, Store, StoreUnavailable

logger = logging.getLogger("messaging")

MAX_BODY_BYTES = 256_000


@dataclass
class Services:
    deps: Deps
    access: Access
    audit: Auditor
    notifier: Notifier
    threads: ThreadService
    tasks: TaskService
    reminders: ReminderService

    async def close(self) -> None:
        for closer in (self.deps.store.close, self.deps.directory.close, self.deps.realtime.close):
            try:
                await closer()
            except Exception:
                logger.exception("error while closing")


def build_store(settings: Settings) -> Store:
    if settings.store == "memory":
        from .storage.memory import MemoryStore
        return MemoryStore(segment_size=settings.segment_size)
    if settings.store == "azure":
        from .storage.azure import AzureStore
        return AzureStore.from_settings(settings)
    raise ValueError(f"unknown MSG_STORE {settings.store!r}")


def build_directory(settings: Settings) -> Directory:
    if settings.directory == "static":
        inner: Directory = StaticDirectory.from_file(settings.directory_static_file)
    elif settings.directory == "webapp":
        inner = WebAppDirectory(base_url=settings.webapp_base_url, scope=settings.webapp_api_scope,
                                dev_key=settings.webapp_dev_key)
    else:
        raise ValueError(f"unknown MSG_DIRECTORY {settings.directory!r}")
    return CachedDirectory(inner, ttl_seconds=settings.directory_cache_seconds)


def build_realtime(settings: Settings) -> RealtimePublisher:
    if settings.realtime == "webpubsub":
        return WebPubSubPublisher(endpoint=settings.webpubsub_endpoint, hub=settings.webpubsub_hub,
                                  connection_string=settings.webpubsub_connection_string)
    return RealtimePublisher()


def build_services(settings: Settings, *, store: Optional[Store] = None, directory: Optional[Directory] = None,
                   realtime: Optional[RealtimePublisher] = None, clock: Optional[Clock] = None,
                   limiter: Optional[RateLimiter] = None) -> Services:
    deps = Deps(settings=settings, store=store or build_store(settings),
                directory=directory or build_directory(settings),
                realtime=realtime or build_realtime(settings), clock=clock or Clock(),
                limiter=limiter or RateLimiter())
    access, audit, notifier = Access(deps), Auditor(deps), Notifier(deps)
    threads = ThreadService(deps, access, audit, notifier)
    tasks = TaskService(deps, access, audit, notifier, threads)
    reminders = ReminderService(deps, notifier, audit)
    return Services(deps, access, audit, notifier, threads, tasks, reminders)


def create_app(settings: Optional[Settings] = None, services: Optional[Services] = None) -> FastAPI:
    settings = settings or get_settings()
    identity, scheme = build_identity_dependency(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if scheme is not None:
            await scheme.openid_config.load_config()
        else:
            logger.warning("MSG_DEV_MODE=true: identity is taken from the X-Dev-User header. Never in production.")
        app.state.services = services or build_services(settings)
        yield
        if services is None:
            await app.state.services.close()

    hide_docs = settings.is_production
    app = FastAPI(title="ScanB Messaging", version="1.0.0", lifespan=lifespan,
                  docs_url=None if hide_docs else "/docs", redoc_url=None,
                  openapi_url=None if hide_docs else "/openapi.json")

    @app.exception_handler(AppError)
    async def app_error(_: Request, exc: AppError):
        headers = {"Retry-After": str(exc.extra["retryAfterSeconds"])} if "retryAfterSeconds" in exc.extra else None
        return JSONResponse(exc.body(), status_code=exc.status, headers=headers)

    @app.exception_handler(ConcurrencyConflict)
    async def conflict(_: Request, __: ConcurrencyConflict):
        return JSONResponse({"code": "busy", "message": "Too many changes at once. Try again."}, status_code=409)

    @app.exception_handler(StoreUnavailable)
    async def unavailable(_: Request, __: StoreUnavailable):
        return JSONResponse({"code": "unavailable", "message": "Messaging is temporarily unavailable."}, status_code=503)

    @app.exception_handler(RequestValidationError)
    async def invalid(_: Request, exc: RequestValidationError):
        fields = [".".join(str(p) for p in e.get("loc", ()) if p != "body") for e in exc.errors()]
        return JSONResponse({"code": "invalid_request", "message": "The request is not valid.", "fields": fields},
                            status_code=422)

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse({"code": "server_error", "message": "Something went wrong. Try again."}, status_code=500)

    @app.middleware("http")
    async def guard(request: Request, call_next):
        length = request.headers.get("content-length")
        if length and length.isdigit() and int(length) > MAX_BODY_BYTES:
            return JSONResponse({"code": "too_large", "message": "The request is too large."}, status_code=413)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    @app.get("/health/live", include_in_schema=False)
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready():
        return {"status": "ok", "store": settings.store, "directory": settings.directory,
                "realtime": settings.realtime}

    app.include_router(build_router(identity))
    return app


def _lazy_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s")
    return create_app()


def __getattr__(name: str):
    # `uvicorn messaging.main:app` builds the app on first access, so importing
    # this module in tests does not require MSG_* settings to exist.
    if name == "app":
        return _lazy_app()
    raise AttributeError(name)
