import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings, get_settings
from app.db import Base, make_engine, make_sessionmaker
from app.models import PROCESSING, Transaction
from app.schemas import Health, TransactionOut, TransactionWebhook, WebhookAck, to_utc_z
from app.worker import TransactionWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("api")


async def keep_alive(url: str, interval: float) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            await asyncio.sleep(interval)
            try:
                await client.get(url)
            except httpx.HTTPError as exc:
                log.warning("keep-alive ping to %s failed: %s", url, exc)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = make_engine(settings.database_url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        sessionmaker = make_sessionmaker(engine)
        worker = TransactionWorker(sessionmaker, settings)
        app.state.engine = engine
        app.state.sessionmaker = sessionmaker
        app.state.worker = worker
        worker.start()
        pinger = None
        if settings.keepalive_url:
            pinger = asyncio.create_task(keep_alive(settings.keepalive_url, settings.keepalive_interval_seconds))
        try:
            yield
        finally:
            if pinger:
                pinger.cancel()
            await worker.stop()
            await engine.dispose()

    app = FastAPI(title="Transaction Webhook Service", version="1.0.0", lifespan=lifespan)

    @app.get("/", response_model=Health)
    async def health() -> Health:
        return Health(status="HEALTHY", current_time=to_utc_z(datetime.now(UTC)))

    @app.post(
        "/v1/webhooks/transactions",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=WebhookAck,
    )
    async def receive_webhook(payload: TransactionWebhook, request: Request) -> WebhookAck:
        sessionmaker = request.app.state.sessionmaker
        worker: TransactionWorker = request.app.state.worker
        values = payload.model_dump() | {
            "status": PROCESSING,
            "created_at": datetime.now(UTC),
            "attempts": 0,
        }
        try:
            async with sessionmaker() as session:
                insert = pg_insert if session.bind.dialect.name == "postgresql" else sqlite_insert
                result = await session.execute(
                    insert(Transaction).values(**values).on_conflict_do_nothing(index_elements=["transaction_id"])
                )
                await session.commit()
        except SQLAlchemyError:
            # Don't ack what we couldn't persist: a non-2xx makes the payment processor retry.
            log.exception("failed to persist webhook %s", payload.transaction_id)
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Could not persist webhook, retry later") from None

        created = result.rowcount == 1
        if created:
            worker.submit(payload.transaction_id)
        else:
            log.info("duplicate webhook for %s ignored", payload.transaction_id)
        return WebhookAck(transaction_id=payload.transaction_id, duplicate=not created)

    @app.get("/v1/transactions/{transaction_id}", response_model=list[TransactionOut])
    async def get_transaction(transaction_id: str, request: Request) -> list[Transaction]:
        async with request.app.state.sessionmaker() as session:
            txn = await session.get(Transaction, transaction_id)
        if txn is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Transaction not found")
        return [txn]

    return app


app = create_app()
