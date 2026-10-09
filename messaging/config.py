"""Settings, read once from the environment (prefix MSG_). See .env.example.

Every choice that differs between a laptop and production lives here, and the
dangerous local conveniences refuse to load in production rather than relying
on somebody remembering to switch them off.
"""
from __future__ import annotations

import base64
from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="MSG_", extra="ignore")

    environment: str = "development"          # development | uat | production

    # Identity from an X-Dev-User header instead of an Azure AD token. Local only.
    dev_mode: bool = False

    # Azure AD: the API app registration this service validates tokens for.
    azure_tenant_id: str = ""
    azure_api_client_id: str = ""
    azure_api_scope_name: str = "access_as_user"

    # Storage. "azure" uses managed identity via the account URLs; a connection
    # string is accepted only outside production (Azurite, local testing).
    store: str = "memory"                     # memory | azure
    storage_blob_url: str = ""
    storage_table_url: str = ""
    storage_connection_string: str = ""

    # Directory: who people are and what they may see. Owned by the Web App.
    directory: str = "static"                 # static | webapp
    directory_static_file: str = "dev/directory.dev.json"
    webapp_base_url: str = ""
    webapp_api_scope: str = ""                # api://<web-app-api-id>/.default
    webapp_dev_key: str = ""                  # local only
    directory_cache_seconds: int = 60

    # Verified context. Comma-separated base64 keys: the first signs (Web App
    # side), all verify, so a key can be rotated without downtime.
    context_signing_keys: str = ""
    context_max_age_seconds: int = 300

    # Live delivery. "poll" means clients fall back to polling.
    realtime: str = "poll"                    # poll | webpubsub
    webpubsub_endpoint: str = ""
    webpubsub_hub: str = "scanbmessaging"
    webpubsub_connection_string: str = ""     # local only

    # Limits
    message_max_chars: int = 4000
    group_max_members: int = 100
    fanout_member_limit: int = 50
    segment_size: int = 200

    # Tasks
    reminder_due_soon_hours: int = 24
    reminder_stale_days: int = 3
    escalate_blocked_days: int = 2
    worker_interval_seconds: int = 300

    @property
    def is_production(self) -> bool:
        return self.environment.strip().lower() == "production"

    def signing_keys(self) -> list[bytes]:
        keys = []
        for raw in self.context_signing_keys.split(","):
            raw = raw.strip()
            if raw:
                keys.append(base64.b64decode(raw))
        return keys

    @model_validator(mode="after")
    def _guards(self) -> "Settings":
        problems = []
        if self.is_production:
            if self.dev_mode:
                problems.append("MSG_DEV_MODE must be false in production")
            if self.store != "azure":
                problems.append("MSG_STORE must be 'azure' in production")
            if self.directory != "webapp":
                problems.append("MSG_DIRECTORY must be 'webapp' in production")
            if self.storage_connection_string:
                problems.append("use managed identity (MSG_STORAGE_*_URL), not a connection string, in production")
            if self.webapp_dev_key:
                problems.append("MSG_WEBAPP_DEV_KEY must not be set in production")
            if self.webpubsub_connection_string:
                problems.append("use MSG_WEBPUBSUB_ENDPOINT with managed identity in production")
        if not self.dev_mode and not (self.azure_tenant_id and self.azure_api_client_id):
            problems.append("MSG_AZURE_TENANT_ID and MSG_AZURE_API_CLIENT_ID are required unless MSG_DEV_MODE=true")
        try:
            keys = self.signing_keys()
        except Exception:
            keys = []
            problems.append("MSG_CONTEXT_SIGNING_KEYS must be base64")
        if not keys or any(len(k) < 32 for k in keys):
            problems.append("MSG_CONTEXT_SIGNING_KEYS needs at least one key of 32+ bytes")
        if problems:
            raise ValueError("; ".join(problems))
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
