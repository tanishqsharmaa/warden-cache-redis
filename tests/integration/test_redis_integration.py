import asyncio
import json
import pytest
from warden_cache.client import WardenCacheClient
from warden_cache.singleflight import SingleFlightCoordinator
from warden_cache.invalidation import CacheInvalidationSubscriber

@pytest.mark.asyncio
async def test_thundering_herd_simulation_50_workers(redis_client):
    """Simulate 50 concurrent requests on an un-cached query. Exactly 1 computation runs."""
    compute_count = 0
    client = WardenCacheClient(redis_client=redis_client)
    coord = SingleFlightCoordinator(redis_client)

    async def worker_task(worker_id: int) -> str:
        nonlocal compute_count
        role = "Employee"
        query = "Parental leave policy duration"

        # 1. Check cache
        cached, _ = await client.get(role, query)
        if cached:
            return cached.answer

        # 2. Cache miss -> acquire mutex
        worker_uuid = f"worker-{worker_id}"
        if await coord.acquire_lock(role, query, worker_uuid, ttl_ms=5000):
            # Winner computes
            compute_count += 1
            await asyncio.sleep(0.05)  # Simulate LLM compute
            answer = "12 weeks of paid leave."
            await client.set(role, query, answer, citations=[], delta_t=0.05, ttl_sec=1800)
            await coord.notify_waiters(role, query, answer)
            await coord.release_lock(role, query, worker_uuid)
            return answer
        else:
            # Losers wait for result via Pub/Sub with fallback to cache check
            result = await coord.wait_for_result(role, query, timeout_sec=3.5)
            assert result is not None, f"Worker {worker_id} timed out without result"
            return result

    # Run 50 workers concurrently
    results = await asyncio.gather(*(worker_task(i) for i in range(50)))

    # Assertions
    assert compute_count == 1, f"Expected exactly 1 computation, got {compute_count}"
    assert len(results) == 50
    assert all(r == "12 weeks of paid leave." for r in results)

    # Subsequent read must be an immediate cache hit with 0 additional computations
    cached_after, should_refresh = await client.get("Employee", "Parental leave policy duration")
    assert cached_after is not None
    assert cached_after.answer == "12 weeks of paid leave."
    assert should_refresh is False

@pytest.mark.asyncio
async def test_xfetch_early_refresh_flow(redis_client):
    """Verify XFetch detects when near-expiry items warrant background refresh."""
    client = WardenCacheClient(redis_client=redis_client)
    role = "Manager"
    query = "Discretionary bonus bands"

    # Store with tiny TTL (1 second) and high delta_t (5.0s)
    await client.set(
        role=role,
        query=query,
        answer="Band C: 15-25%",
        delta_t=5.0,
        ttl_sec=1,
    )

    env, should_refresh = await client.get(role, query, beta=2.0)
    assert env is not None
    assert env.answer == "Band C: 15-25%"
    # Because remaining TTL <= 1s and delta_t = 5s, early refresh probability is near 100%
    assert should_refresh is True

@pytest.mark.asyncio
async def test_cache_invalidation_end_to_end(redis_client):
    """Verify that emitting invalidation for 'Employee' purges only Employee cache keys."""
    client = WardenCacheClient(redis_client=redis_client)
    sub = CacheInvalidationSubscriber(redis_client)

    # 1. Populate both Employee and Manager cache entries
    await client.set("Employee", "PTO allowance", "18 days")
    await client.set("Manager", "PTO allowance", "25 days")

    # Verify both are initially present
    emp_before, _ = await client.get("Employee", "PTO allowance")
    mgr_before, _ = await client.get("Manager", "PTO allowance")
    assert emp_before is not None
    assert mgr_before is not None

    # 2. Trigger invalidation event for Employee only
    event_payload = json.dumps({
        "event": "INGESTION_COMPLETED",
        "run_id": "batch-101",
        "affected_roles": ["Employee"],
        "timestamp": "2026-09-29T00:00:00Z"
    })
    purged_roles = await sub.handle_invalidation_payload(event_payload)
    assert purged_roles == ["Employee"]

    # 3. Verify Employee key was evicted, Manager key remains intact
    emp_after, _ = await client.get("Employee", "PTO allowance")
    mgr_after, _ = await client.get("Manager", "PTO allowance")
    assert emp_after is None, "Employee cache key was not purged!"
    assert mgr_after is not None, "Manager cache key was incorrectly purged!"
    assert mgr_after.answer == "25 days"

@pytest.mark.asyncio
async def test_client_ping_and_lifecycle(redis_client):
    """Verify ping and lifecycle connectivity."""
    client = WardenCacheClient(redis_client=redis_client)
    alive = await client.ping()
    assert alive is True
