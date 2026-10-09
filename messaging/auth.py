"""Who is calling.

Production: an Azure AD access token for this API, delegated (it must carry
the `access_as_user` scope, so an application token cannot act as a person).
The email comes from the token and nowhere else.

Development (MSG_DEV_MODE=true): the email comes from an X-Dev-User header.
Settings refuse to load this in production.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from fastapi import Header, Security

from .config import Settings
from .errors import AppError


@dataclass(frozen=True)
class Identity:
    email: str


class Unauthenticated(AppError):
    status = 401
    code = "unauthenticated"


def build_identity_dependency(settings: Settings) -> tuple[Callable, Optional[object]]:
    """Returns (dependency, azure_scheme_or_None). The scheme must have its
    OpenID configuration loaded at startup."""
    if settings.dev_mode:
        async def dev_identity(x_dev_user: Optional[str] = Header(default=None)) -> Identity:
            if not x_dev_user or "@" not in x_dev_user:
                raise Unauthenticated("Sign in to continue.")
            return Identity(email=x_dev_user.strip().lower())
        return dev_identity, None

    from fastapi_azure_auth import SingleTenantAzureAuthorizationCodeBearer

    scope_uri = f"api://{settings.azure_api_client_id}/{settings.azure_api_scope_name}"
    scheme = SingleTenantAzureAuthorizationCodeBearer(
        app_client_id=settings.azure_api_client_id,
        tenant_id=settings.azure_tenant_id,
        # Guests are allowed at the token layer because ScanB customers include
        # guest accounts; whether a person may use messaging is the directory's
        # decision, made on the next line of every request.
        allow_guest_users=True,
        scopes={scope_uri: "Use ScanB Messaging as the signed-in user"},
        auto_error=True,
    )

    async def azure_identity(user=Security(scheme, scopes=[settings.azure_api_scope_name])) -> Identity:
        claims = getattr(user, "claims", None) or {}
        email = claims.get("preferred_username") or claims.get("upn") or claims.get("email")
        if not email:
            raise Unauthenticated("The sign-in token does not identify a person.")
        return Identity(email=str(email).strip().lower())

    return azure_identity, scheme
