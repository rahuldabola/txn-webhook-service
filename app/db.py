from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


def normalize_database_url(url: str) -> tuple[str, dict]:
    """Return (sqlalchemy_url, connect_args) for the configured database.

    Hosted Postgres providers hand out libpq-style URLs (postgres://...?sslmode=require).
    asyncpg doesn't understand libpq query params, so they are translated here.
    """
    if url.startswith("sqlite"):
        return url, {}

    scheme, netloc, path, query, fragment = urlsplit(url)
    if scheme in ("postgres", "postgresql"):
        scheme = "postgresql+asyncpg"

    params = dict(parse_qsl(query))
    connect_args: dict = {
        # Safe behind PgBouncer-style poolers (Neon/Supabase pooled endpoints).
        "statement_cache_size": 0,
    }
    sslmode = params.pop("sslmode", None)
    params.pop("channel_binding", None)
    if sslmode and sslmode != "disable":
        connect_args["ssl"] = "require"

    return urlunsplit((scheme, netloc, path, urlencode(params), fragment)), connect_args


def make_engine(url: str) -> AsyncEngine:
    sa_url, connect_args = normalize_database_url(url)
    if sa_url.startswith("sqlite"):
        return create_async_engine(sa_url, connect_args={"timeout": 30})
    return create_async_engine(
        sa_url,
        connect_args=connect_args,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=300,
    )


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)
