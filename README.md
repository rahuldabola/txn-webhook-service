# Transaction Webhook Service

A Python service that receives transaction webhooks from payment processors (such as Razorpay), acknowledges them immediately with `202 Accepted`, and processes each transaction reliably in the background. Processing includes a simulated 30-second external API call.

**Live API:** `LIVE_URL`

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/webhooks/transactions` | Accepts a webhook and returns `202` right away |
| `GET` | `/` | Health check: `{"status": "HEALTHY", "current_time": "..."}` |
| `GET` | `/v1/transactions/{transaction_id}` | Returns the transaction's status and timing (for testing) |
| `GET` | `/docs` | Interactive OpenAPI docs |

## Quick test against the live service

```bash
URL=LIVE_URL

# 1. Send a webhook. You get 202 immediately.
curl -i -X POST $URL/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

# 2. Check it right away: status is PROCESSING and processed_at is null.
curl $URL/v1/transactions/txn_abc123def456

# 3. Send the same webhook again: still 202, flagged "duplicate": true, and nothing is processed twice.

# 4. After about 30 seconds: status is PROCESSED and processed_at is set.
curl $URL/v1/transactions/txn_abc123def456
```

Or run the full end-to-end check, which covers all four success criteria:

```bash
pip install httpx
python scripts/smoke_test.py LIVE_URL
```

The script tests the health check, a single transaction processed after about 30 seconds, four duplicate deliveries (sent one after another and at the same time), webhook latency while 50 transactions are processing, rejection of invalid payloads, and a 404 for unknown IDs.

## Run locally

Requires Python 3.11 or later.

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
```

With no `DATABASE_URL` set, the service uses a local SQLite file, so it runs with zero setup. To use Postgres, copy `.env.example` to `.env` and set `DATABASE_URL` (a plain `postgresql://...?sslmode=require` URL from Neon or Supabase works as-is).

```bash
pytest -q                                      # 17 tests, about 20 s (processing delay shortened to 0.5 s)
python scripts/smoke_test.py http://localhost:8000
```

Docker: `docker build -t txn-webhooks . && docker run -p 8000:8000 -e DATABASE_URL=... txn-webhooks`

### Configuration

| Env var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | local SQLite | Any Postgres URL (converted to `asyncpg` automatically) |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external API latency |
| `LEASE_SECONDS` | `60` | How long a worker owns a claimed transaction before others may retry it |
| `SWEEP_INTERVAL_SECONDS` | `5` | How often the recovery sweeper looks for orphaned or retryable rows |
| `MAX_CONCURRENCY` | `500` | Transactions processed at once per instance |
| `MAX_ATTEMPTS` | `5` | Retries before a transaction is parked as `FAILED` |
| `KEEPALIVE_URL` | `RENDER_EXTERNAL_URL` | URL the app pings to stop a free host from sleeping |

## How it works

```
payment processor ──POST──▶ FastAPI ──INSERT … ON CONFLICT DO NOTHING──▶ Postgres (transactions)
                              │  ◀── 202 Accepted (~tens of ms)                ▲
                              └─ new row? start a task ─▶ worker: claim lease ─┤
                                                           sleep 30 s          │
                                                           mark PROCESSED ─────┘
                   sweeper (every 5 s): rows still PROCESSING with no live lease ─▶ worker
```

1. **Acknowledge fast.** The webhook handler validates the payload, does one indexed `INSERT … ON CONFLICT (transaction_id) DO NOTHING`, and returns `202`. The 30-second work never runs in the request path, so latency is one database round trip no matter how much processing is in flight. Measured at 27 ms median and 46 ms max locally while 50 transactions were processing.
2. **Persist before acknowledging.** The row is committed before the `202` goes out. If the database is unreachable, the handler returns `503` instead of acknowledging, so the payment processor's own retry kicks in. A webhook is never acknowledged and then lost.
3. **The table is the queue.** Each row starts as `PROCESSING`. A worker claims a row with a conditional `UPDATE … WHERE status='PROCESSING' AND (locked_until IS NULL OR locked_until < now)`, which sets a lease and increments `attempts`. Only one claimer can win that update, even across several app instances.
4. **Finish with a fence.** After the simulated external call, the row is set to `PROCESSED` with `processed_at` only `WHERE attempts = <my attempt>`. A worker whose lease expired and was taken over can never complete the transaction a second time.
5. **Recover everything.** A background sweeper picks up any row still `PROCESSING` without a live lease. That covers a crashed instance (its lease expires), a redeploy (graceful shutdown hands leases back immediately), work over the concurrency limit, and failed attempts waiting out their retry backoff.

### Idempotency

`transaction_id` is the primary key. Duplicate deliveries hit `ON CONFLICT DO NOTHING`, still receive `202` (so the processor stops retrying), and the response includes `"duplicate": true`. Only the first delivery schedules work, and the lease plus fence guarantees a single processing run even under concurrent delivery and multiple instances. Tests cover 10 identical webhooks sent concurrently: exactly one processing run.

### Failure handling

| Scenario | What happens |
|---|---|
| Processing step raises an exception | Error is recorded in `last_error` and retried with exponential backoff (5 s, 10 s, 20 s, …). After `MAX_ATTEMPTS` the row is marked `FAILED` |
| Process crashes or is killed mid-processing | Lease expires after `LEASE_SECONDS` and the sweeper retries. Verified by killing the server with `taskkill /F`: the transaction was reprocessed on attempt 2 |
| Graceful shutdown or redeploy | In-flight tasks are cancelled, leases are released, and the attempt is refunded, so the next instance resumes immediately |
| Database down during a webhook | `503`, which makes the processor retry. Nothing is acknowledged that isn't stored |
| Malformed payload | `422` with field errors (missing fields, amount ≤ 0, currency not 3 letters) |

## Technical choices

- **FastAPI + Uvicorn (asyncio).** The workload is I/O-bound (the database, and a 30-second wait standing in for an external API), so a single async process handles hundreds of concurrent transactions without threads. Pydantic gives request validation for free.
- **Postgres (Neon) as both store and queue.** The assignment needs persistent storage anyway. Using the same table as the job queue means the webhook acknowledgement and the job enqueue are one atomic write, with no dual-write problem between a database and a broker. It also means there's no Redis or Celery worker to run and pay for. Leases with a fencing token give at-least-once execution with exactly-once completion. The same claim query works with several instances, so it scales horizontally. At much higher volume I would move to `SELECT … FOR UPDATE SKIP LOCKED` batch claims or a managed queue (SQS, Cloud Tasks) with the same idempotent consumer.
- **SQLAlchemy 2 async + asyncpg.** The code is portable between Postgres in production and SQLite for local runs and tests, so `pytest` needs no external services.
- **Render (Docker) + Neon.** The service needs a long-running process for background work, which rules out pure serverless. Render's free tier sleeps after 15 idle minutes, which would add cold-start latency far above 500 ms. The app pings its own public URL every 5 minutes to stay awake, and a GitHub Actions cron is configured as a backup. Both are in the same region (Singapore) to keep database round trips short.
- **Amounts** are stored as `NUMERIC(20,4)` (never floats) and returned as JSON numbers. The currency is upper-cased.

## Project layout

```
app/
  main.py      FastAPI app, endpoints, lifespan (schema creation, worker start/stop)
  worker.py    lease-based background processor and recovery sweeper
  models.py    transactions table
  schemas.py   request/response models
  db.py        engine setup, libpq to asyncpg URL handling
  config.py    settings from environment variables
tests/         17 tests: idempotency (sequential and concurrent), retries, crash recovery, shutdown, latency, validation
scripts/smoke_test.py   end-to-end check against any deployment
```
