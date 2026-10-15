from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI

from src.infrastructure.http.auth_client import AuthServiceUserProfileService
from src.infrastructure.http.tracing import inject_trace_id
from src.infrastructure.persistence.database import (
    create_engine,
    create_session_factory,
)
from src.presentation.api.dependencies import setup
from src.presentation.api.middleware import TraceIdMiddleware
from src.presentation.api.routes.internal import router as internal_router
from src.presentation.api.routes.public import router as public_router
from src.settings import Settings


def create_app() -> FastAPI:
    settings = Settings()

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with httpx.AsyncClient(
            timeout=5.0,
            event_hooks={"request": [inject_trace_id]},
        ) as client:
            user_profile = AuthServiceUserProfileService(
                client,
                settings.auth_service_url,
            )
            setup(settings, session_factory, user_profile)
            yield

    app = FastAPI(title="Ad Service", lifespan=lifespan)
    app.add_middleware(TraceIdMiddleware)
    app.include_router(public_router)
    app.include_router(internal_router)
    return app
