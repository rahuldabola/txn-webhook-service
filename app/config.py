from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Any SQLAlchemy URL. Plain postgres:// / postgresql:// URLs (as copied from
    # Neon, Supabase, Render, ...) are rewritten to the asyncpg driver.
    database_url: str = "sqlite+aiosqlite:///./transactions.db"

    # Simulated external-API latency for each transaction.
    processing_delay_seconds: float = 30.0

    # How long a worker owns a claimed transaction. If the process dies mid-way,
    # the lease expires and another worker (or this one, after restart) retries it.
    lease_seconds: float = 60.0

    # How often the recovery sweeper looks for unclaimed / expired-lease rows.
    sweep_interval_seconds: float = 5.0

    # Upper bound on transactions being processed concurrently by one instance.
    max_concurrency: int = 500

    # After this many failed attempts a transaction is parked as FAILED.
    max_attempts: int = 5

    # Retry delay after a failed attempt: base * 2^(attempt-1), capped at 5 minutes.
    retry_backoff_seconds: float = 5.0

    # Free hosting tiers (Render) put the service to sleep after ~15 min without inbound
    # traffic, which would stall background work and blow the 500ms budget on wake-up.
    # When set, the app pings its own public URL to stay awake. Render injects
    # RENDER_EXTERNAL_URL automatically.
    keepalive_url: str | None = Field(
        default=None, validation_alias=AliasChoices("KEEPALIVE_URL", "RENDER_EXTERNAL_URL")
    )
    keepalive_interval_seconds: float = 300.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
