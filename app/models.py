from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base

PROCESSING = "PROCESSING"
PROCESSED = "PROCESSED"
FAILED = "FAILED"


class Transaction(Base):
    __tablename__ = "transactions"

    # The primary key on transaction_id is what makes webhook ingestion idempotent:
    # a duplicate delivery can never create a second row.
    transaction_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_account: Mapped[str] = mapped_column(String(128))
    destination_account: Mapped[str] = mapped_column(String(128))
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 4))
    currency: Mapped[str] = mapped_column(String(3))

    status: Mapped[str] = mapped_column(String(16), default=PROCESSING)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Work-queue bookkeeping.
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (Index("ix_transactions_status_locked_until", "status", "locked_until"),)
