from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


def to_utc_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:  # SQLite drops tzinfo; everything is stored as UTC.
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class TransactionWebhook(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    transaction_id: str = Field(min_length=1, max_length=128)
    source_account: str = Field(min_length=1, max_length=128)
    destination_account: str = Field(min_length=1, max_length=128)
    amount: Decimal = Field(gt=0, max_digits=20, decimal_places=4)
    currency: str = Field(pattern=r"^[A-Za-z]{3}$")

    @field_validator("currency")
    @classmethod
    def upper_currency(cls, v: str) -> str:
        return v.upper()


class WebhookAck(BaseModel):
    status: str = "ACCEPTED"
    transaction_id: str
    duplicate: bool


class TransactionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    source_account: str
    destination_account: str
    amount: Decimal
    currency: str
    status: str
    created_at: datetime
    processed_at: datetime | None

    @field_serializer("amount")
    def ser_amount(self, v: Decimal) -> int | float:
        return int(v) if v == v.to_integral_value() else float(v)

    @field_serializer("created_at", "processed_at")
    def ser_dt(self, v: datetime | None) -> str | None:
        return to_utc_z(v)


class Health(BaseModel):
    status: str
    current_time: str
