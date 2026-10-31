"""End-to-end check of a running deployment against the assessment's success criteria.

    python scripts/smoke_test.py https://your-service.onrender.com

1. Single transaction: processed after ~30s
2. Duplicate prevention: same webhook sent 5x (sequential + concurrent) -> one transaction
3. Performance: webhook latency stays < 500ms while 50 transactions are processing
4. Reliability: invalid payloads are rejected cleanly; unknown ids return 404
"""

import asyncio
import statistics
import sys
import time
import uuid

import httpx

BUDGET_MS = 500


def payload(txn_id: str) -> dict:
    return {
        "transaction_id": txn_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }


async def post(client: httpx.AsyncClient, body: dict) -> tuple[httpx.Response, float]:
    start = time.perf_counter()
    r = await client.post("/v1/webhooks/transactions", json=body)
    return r, (time.perf_counter() - start) * 1000


async def main(base_url: str) -> int:
    failures = 0

    def check(ok: bool, msg: str) -> None:
        nonlocal failures
        print(("PASS " if ok else "FAIL ") + msg)
        failures += not ok

    run = uuid.uuid4().hex[:8]
    async with httpx.AsyncClient(base_url=base_url, timeout=30) as client:
        r = await client.get("/")
        check(r.status_code == 200 and r.json().get("status") == "HEALTHY", f"health: {r.json()}")

        # 2 + 1: duplicates, which also serves as the single-transaction test
        txn = f"txn_smoke_{run}"
        first, ms = await post(client, payload(txn))
        check(first.status_code == 202, f"webhook -> {first.status_code} in {ms:.0f}ms")
        dupes = [await post(client, payload(txn)) for _ in range(2)]
        dupes += await asyncio.gather(*(post(client, payload(txn)) for _ in range(2)))
        check(
            all(r.status_code == 202 and r.json()["duplicate"] for r, _ in dupes),
            "4 duplicate deliveries all acknowledged with 202 and flagged duplicate",
        )
        state = (await client.get(f"/v1/transactions/{txn}")).json()
        check(len(state) == 1 and state[0]["status"] == "PROCESSING", f"immediately after: {state[0]['status']}")

        # 3: performance while lots of work is in flight
        load = await asyncio.gather(*(post(client, payload(f"txn_load_{run}_{i}")) for i in range(50)))
        probes = [(await post(client, payload(f"txn_probe_{run}_{i}")))[1] for i in range(10)]
        check(all(r.status_code == 202 for r, _ in load), "50 concurrent webhooks accepted")
        check(
            max(probes) < BUDGET_MS,
            f"webhook latency under load: median {statistics.median(probes):.0f}ms, max {max(probes):.0f}ms",
        )

        # 4: reliability / error handling
        bad = await client.post("/v1/webhooks/transactions", json={"transaction_id": "x"})
        check(bad.status_code == 422, f"invalid payload -> {bad.status_code}")
        missing = await client.get(f"/v1/transactions/does_not_exist_{run}")
        check(missing.status_code == 404, f"unknown transaction -> {missing.status_code}")

        print("waiting for background processing (~30s)...")
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            state = (await client.get(f"/v1/transactions/{txn}")).json()
            if state[0]["status"] != "PROCESSING":
                break
            await asyncio.sleep(2)
        print(state)
        check(state[0]["status"] == "PROCESSED", "single transaction processed")
        check(state[0]["processed_at"] is not None, f"processed_at = {state[0]['processed_at']}")

        while True:
            load_states = await asyncio.gather(*(client.get(f"/v1/transactions/txn_load_{run}_{i}") for i in range(50)))
            processed = sum(r.json()[0]["status"] == "PROCESSED" for r in load_states)
            if processed == 50 or time.monotonic() > deadline:
                break
            await asyncio.sleep(2)
        check(processed == 50, f"{processed}/50 load transactions processed")

    print("\nALL PASSED" if not failures else f"\n{failures} CHECK(S) FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000")))
