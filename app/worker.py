"""Background processing backed by the transactions table itself.

The database row *is* the job. Rows in PROCESSING state are claimed with a lease
(locked_until) via a conditional UPDATE, so:

* a transaction is only ever worked on by one worker at a time, even with several
  app instances running against the same database;
* if the process crashes or is redeployed mid-processing, the lease simply expires
  and the sweeper picks the row up again - nothing is lost;
* completion is fenced on the attempt number, so a worker whose lease was taken
  over can never mark the row processed a second time.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Settings
from app.models import FAILED, PROCESSED, PROCESSING, Transaction

log = logging.getLogger("worker")

Processor = Callable[[Transaction], Awaitable[None]]


def utcnow() -> datetime:
    return datetime.now(UTC)


class TransactionWorker:
    def __init__(self, sessionmaker: async_sessionmaker, settings: Settings, processor: Processor | None = None):
        self.sessionmaker = sessionmaker
        self.settings = settings
        self.processor = processor or self.simulate_external_call
        self._inflight: dict[str, asyncio.Task] = {}
        self._sweeper: asyncio.Task | None = None

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        self._sweeper = asyncio.create_task(self._sweep_forever(), name="sweeper")

    async def stop(self) -> None:
        if self._sweeper:
            self._sweeper.cancel()
            await asyncio.gather(self._sweeper, return_exceptions=True)
        tasks = list(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def capacity(self) -> int:
        return max(self.settings.max_concurrency - len(self._inflight), 0)

    # -- scheduling ------------------------------------------------------------

    def submit(self, transaction_id: str) -> bool:
        """Start processing in this process if there is room; otherwise the sweeper will get to it."""
        if transaction_id in self._inflight or self.capacity == 0:
            return False
        task = asyncio.create_task(self._run(transaction_id), name=f"txn:{transaction_id}")
        self._inflight[transaction_id] = task
        task.add_done_callback(lambda _: self._inflight.pop(transaction_id, None))
        return True

    async def _sweep_forever(self) -> None:
        while True:
            try:
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("sweep failed")
            await asyncio.sleep(self.settings.sweep_interval_seconds)

    async def sweep(self) -> int:
        """Pick up rows nobody is working on: new ones that weren't started, expired leases, retries."""
        if self.capacity == 0:
            return 0
        now = utcnow()
        async with self.sessionmaker() as session:
            ids = (
                await session.scalars(
                    select(Transaction.transaction_id)
                    .where(
                        Transaction.status == PROCESSING,
                        or_(Transaction.locked_until.is_(None), Transaction.locked_until < now),
                    )
                    .order_by(Transaction.created_at)
                    .limit(self.capacity + len(self._inflight))
                )
            ).all()
        return sum(self.submit(i) for i in ids)

    # -- processing ------------------------------------------------------------

    async def _claim(self, transaction_id: str) -> Transaction | None:
        now = utcnow()
        async with self.sessionmaker() as session:
            result = await session.execute(
                update(Transaction)
                .where(
                    Transaction.transaction_id == transaction_id,
                    Transaction.status == PROCESSING,
                    or_(Transaction.locked_until.is_(None), Transaction.locked_until < now),
                )
                .values(
                    locked_until=now + timedelta(seconds=self.settings.lease_seconds),
                    attempts=Transaction.attempts + 1,
                )
            )
            await session.commit()
            if result.rowcount != 1:
                return None
            return await session.get(Transaction, transaction_id, populate_existing=True)

    async def _run(self, transaction_id: str) -> None:
        txn = await self._claim(transaction_id)
        if txn is None:
            return  # already processed, or another worker holds the lease
        attempt = txn.attempts
        owned = and_(
            Transaction.transaction_id == transaction_id,
            Transaction.status == PROCESSING,
            Transaction.attempts == attempt,
        )
        log.info("processing %s (attempt %d)", transaction_id, attempt)
        try:
            await self.processor(txn)
        except asyncio.CancelledError:
            # Shutting down: hand the row straight back instead of waiting for the lease to expire.
            await self._update(owned, locked_until=None, attempts=attempt - 1)
            raise
        except Exception as exc:
            log.exception("processing %s failed", transaction_id)
            if attempt >= self.settings.max_attempts:
                await self._update(owned, status=FAILED, locked_until=None, last_error=repr(exc))
            else:
                backoff = min(self.settings.retry_backoff_seconds * 2 ** (attempt - 1), 300)
                await self._update(owned, locked_until=utcnow() + timedelta(seconds=backoff), last_error=repr(exc))
            return

        done = await self._update(owned, status=PROCESSED, processed_at=utcnow(), locked_until=None, last_error=None)
        if done:
            log.info("processed %s", transaction_id)
        else:
            log.warning("lost lease on %s before completion; result discarded", transaction_id)

    async def _update(self, condition, **values) -> bool:
        async with self.sessionmaker() as session:
            result = await session.execute(update(Transaction).where(condition).values(**values))
            await session.commit()
            return result.rowcount == 1

    async def simulate_external_call(self, txn: Transaction) -> None:
        await asyncio.sleep(self.settings.processing_delay_seconds)
