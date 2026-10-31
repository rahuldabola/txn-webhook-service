import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import normalize_database_url
from app.main import create_app
from app.models import Transaction

PAYLOAD = {
    "transaction_id": "txn_abc123def456",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
}

DELAY = 0.5


@pytest.fixture
def settings(tmp_path):
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'test.db'}",
        processing_delay_seconds=DELAY,
        sweep_interval_seconds=0.1,
        retry_backoff_seconds=0.1,
        lease_seconds=5,
    )


@pytest.fixture
def client(settings):
    with TestClient(create_app(settings)) as c:
        yield c


def count_calls(client):
    """Wrap the worker's processor so tests can see how many times it actually ran."""
    worker = client.app.state.worker
    original = worker.processor
    calls = []

    async def counting(txn):
        calls.append(txn.transaction_id)
        await original(txn)

    worker.processor = counting
    return calls


def wait_for_status(client, txn_id, wanted, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/v1/transactions/{txn_id}")
        if r.status_code == 200 and r.json()[0]["status"] == wanted:
            return r.json()[0]
        time.sleep(0.05)
    raise AssertionError(f"{txn_id} never reached {wanted}")


def test_health(client):
    r = client.get("/")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "HEALTHY"
    datetime.strptime(body["current_time"], "%Y-%m-%dT%H:%M:%SZ")


def test_single_transaction_is_processed_after_delay(client):
    r = client.post("/v1/webhooks/transactions", json=PAYLOAD)
    assert r.status_code == 202
    assert r.json() == {"status": "ACCEPTED", "transaction_id": PAYLOAD["transaction_id"], "duplicate": False}

    first = client.get(f"/v1/transactions/{PAYLOAD['transaction_id']}").json()[0]
    assert first["status"] == "PROCESSING"
    assert first["processed_at"] is None

    done = wait_for_status(client, PAYLOAD["transaction_id"], "PROCESSED")
    assert done["amount"] == 1500
    assert done["currency"] == "INR"
    created = datetime.strptime(done["created_at"], "%Y-%m-%dT%H:%M:%SZ")
    processed = datetime.strptime(done["processed_at"], "%Y-%m-%dT%H:%M:%SZ")
    # second-resolution timestamps, so allow one second of rounding
    assert processed >= created
    assert (processed - created) <= timedelta(seconds=DELAY + 2)


def test_duplicate_webhooks_process_once(client):
    calls = count_calls(client)
    responses = [client.post("/v1/webhooks/transactions", json=PAYLOAD) for _ in range(5)]
    assert [r.status_code for r in responses] == [202] * 5
    assert [r.json()["duplicate"] for r in responses] == [False, True, True, True, True]

    wait_for_status(client, PAYLOAD["transaction_id"], "PROCESSED")
    time.sleep(DELAY * 2)  # give any (wrong) second run a chance to happen
    assert calls == [PAYLOAD["transaction_id"]]


def test_concurrent_duplicates_process_once(client):
    calls = count_calls(client)
    payload = PAYLOAD | {"transaction_id": "txn_concurrent"}
    statuses = []

    def send():
        statuses.append(client.post("/v1/webhooks/transactions", json=payload).json()["duplicate"])

    threads = [threading.Thread(target=send) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses) == [False] + [True] * 9
    wait_for_status(client, "txn_concurrent", "PROCESSED")
    time.sleep(DELAY * 2)
    assert calls == ["txn_concurrent"]


def test_webhook_is_fast_under_load(client):
    for i in range(200):
        client.post("/v1/webhooks/transactions", json=PAYLOAD | {"transaction_id": f"txn_load_{i}"})
    start = time.perf_counter()
    r = client.post("/v1/webhooks/transactions", json=PAYLOAD | {"transaction_id": "txn_probe"})
    assert r.status_code == 202
    assert time.perf_counter() - start < 0.5


def test_failed_processing_is_retried(client):
    worker = client.app.state.worker
    original = worker.processor
    attempts = []

    async def flaky(txn):
        attempts.append(txn.attempts)
        if len(attempts) == 1:
            raise RuntimeError("upstream timeout")
        await original(txn)

    worker.processor = flaky
    client.post("/v1/webhooks/transactions", json=PAYLOAD)
    wait_for_status(client, PAYLOAD["transaction_id"], "PROCESSED")
    assert attempts == [1, 2]


def test_gives_up_after_max_attempts(settings):
    settings.max_attempts = 2
    with TestClient(create_app(settings)) as client:

        async def broken(txn):
            raise RuntimeError("always down")

        client.app.state.worker.processor = broken
        client.post("/v1/webhooks/transactions", json=PAYLOAD)
        done = wait_for_status(client, PAYLOAD["transaction_id"], "FAILED")
        assert done["processed_at"] is None


def test_transactions_survive_a_crash(settings):
    """A row left mid-processing by a dead instance is picked up after restart."""
    with TestClient(create_app(settings)) as client:

        async def seed():
            async with client.app.state.sessionmaker() as session:
                session.add(
                    Transaction(
                        **(PAYLOAD | {"transaction_id": "txn_orphan"}),
                        status="PROCESSING",
                        created_at=datetime.now(UTC) - timedelta(minutes=5),
                        attempts=1,
                        # lease held by a worker that died
                        locked_until=datetime.now(UTC) - timedelta(seconds=1),
                    )
                )
                await session.commit()

        client.portal.call(seed)
        done = wait_for_status(client, "txn_orphan", "PROCESSED")
        assert done["processed_at"] is not None


def test_claim_is_exclusive(client):
    client.post("/v1/webhooks/transactions", json=PAYLOAD | {"transaction_id": "txn_claim"})
    worker = client.app.state.worker

    async def claim_twice():
        # the webhook already claimed it, so further claims must fail while the lease is live
        return await worker._claim("txn_claim"), await worker._claim("txn_claim")

    assert client.portal.call(claim_twice) == (None, None)


def test_shutdown_releases_inflight_work(settings):
    settings.processing_delay_seconds = 60
    with TestClient(create_app(settings)) as client:
        client.post("/v1/webhooks/transactions", json=PAYLOAD)
        time.sleep(0.2)
    # lifespan exited: the row should be immediately claimable again, with the attempt refunded
    settings.processing_delay_seconds = DELAY
    with TestClient(create_app(settings)) as client:
        done = wait_for_status(client, PAYLOAD["transaction_id"], "PROCESSED", timeout=3)
        assert done["status"] == "PROCESSED"


@pytest.mark.parametrize(
    "body",
    [
        {k: v for k, v in PAYLOAD.items() if k != "amount"},
        PAYLOAD | {"amount": -5},
        PAYLOAD | {"currency": "RUPEES"},
        PAYLOAD | {"transaction_id": ""},
    ],
)
def test_invalid_payload_rejected(client, body):
    assert client.post("/v1/webhooks/transactions", json=body).status_code == 422


def test_unknown_transaction_404(client):
    assert client.get("/v1/transactions/nope").status_code == 404


def test_decimal_amount_round_trips(client):
    payload = PAYLOAD | {"transaction_id": "txn_usd", "amount": 150.50, "currency": "usd"}
    client.post("/v1/webhooks/transactions", json=payload)
    body = client.get("/v1/transactions/txn_usd").json()[0]
    assert body["amount"] == 150.5
    assert body["currency"] == "USD"


def test_normalize_database_url():
    url, args = normalize_database_url("postgres://u:p@host.neon.tech/db?sslmode=require&channel_binding=require")
    assert url == "postgresql+asyncpg://u:p@host.neon.tech/db"
    assert args == {"statement_cache_size": 0, "ssl": "require"}
    assert normalize_database_url("sqlite+aiosqlite:///x.db") == ("sqlite+aiosqlite:///x.db", {})
