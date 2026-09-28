# Project Warden Distributed Cache (`warden-cache-redis`)
## Autonomous Enterprise Hybrid-RAG Distributed Caching Engine (Tier 1)

`warden-cache-redis` is the Tier 1 distributed caching engine and client library for **Project Warden**. It provides high-availability Redis 7.2 Sentinel deployment manifests, zero-trust network isolation, and client-side Python libraries implementing role-partitioned cache key security, the XFetch probabilistic early expiration algorithm (eliminating cache stampedes), distributed SingleFlight mutual exclusion locks, and event-driven cache invalidation.

---

## 1. Architectural Highlights

- **Two-Tier Caching Foundation**: Works in tandem with `warden-orchestrator`'s in-memory L1 LRU cache (1,000 entries, 60s TTL) and serves as the distributed L2 cache (1,800s TTL) for answers, embeddings, and reranker outputs.
- **Cache Stampede Elimination (XFetch)**: Implements the probabilistic early expiration algorithm:
  $$\Delta = -\beta \cdot \delta \cdot \ln(U)$$
  As cached items approach expiration, the probability of background refresh scales smoothly to 1.0, ensuring exactly one worker recomputes before TTL expiry.
- **Distributed SingleFlight Mutex**: On cache misses, concurrent identical requests acquire a distributed lock (`SET lock:query:{role}:{hash} {worker_uuid} NX PX 5000`) with atomic Lua script release. Contending workers wait on Redis Pub/Sub channels with fallback to cache checks.
- **Zero-Trust Role-Partitioned Keys**: Cache keys strictly bind the caller's security role:
  `cache:query:{role_tier}:{sha256(normalize(query_text))}`
  Guarantees that an `Employee` query can never hit `Manager` or `HR-Admin` cached context.
- **Non-Blocking Invalidation**: Subscribes to `warden:cache:invalidate` and purges matching keys using asynchronous `SCAN` (batches of 100) and `UNLINK` to eliminate Redis main event loop stalls.
- **Fail-Open Resilience**: Gracefully degrades to live retrieval with operational metrics on Redis connectivity failures.

---

## 2. Directory Structure

```
warden-cache-redis/
├── k8s/
│   ├── redis/
│   │   ├── redis-configmap.yaml         # Redis memory (512MB LRU) and AOF persistence
│   │   ├── redis-statefulset.yaml       # Redis 7.2 Sentinel StatefulSet (5Gi PVC)
│   │   ├── sentinel-deployment.yaml     # 3-replica Redis Sentinel deployment
│   │   └── redis-service.yaml           # Headless and ClusterIP services
│   └── network-policies/
│       └── isolate-redis.yaml           # Zero-trust NetworkPolicy (isolate-redis-cache)
├── src/
│   └── warden_cache/
│       ├── __init__.py                  # Package exports
│       ├── keys.py                      # Query normalization & role-partitioned keys
│       ├── xfetch.py                    # XFetch probabilistic expiration & envelope
│       ├── singleflight.py              # Distributed mutex & Pub/Sub coordination
│       ├── invalidation.py              # Async SCAN + UNLINK invalidation subscriber
│       └── client.py                    # Unified async WardenCacheClient with Sentinel
├── tests/
│   ├── conftest.py                      # Shared pytest async fixtures
│   ├── unit/
│   │   ├── test_k8s_manifests.py        # Manifest syntax and directive validation
│   │   ├── test_network_policy.py       # Zero-trust NetworkPolicy isolation tests
│   │   ├── test_cache_keys.py           # Role namespacing and normalization tests
│   │   ├── test_xfetch.py               # XFetch formula and envelope tests
│   │   ├── test_singleflight.py         # Mutex and atomic Lua release tests
│   │   ├── test_invalidation.py         # SCAN + UNLINK invalidation tests
│   │   └── test_client.py               # High-level client and fail-open tests
│   └── integration/
│       └── test_redis_integration.py    # 50-worker thundering herd simulation
└── pyproject.toml                       # Python package configuration
```

---

## 3. Quickstart: Installation & Development

```bash
# 1. Initialize Python 3.12 virtual environment
uv venv .venv --python 3.12
.venv\Scripts\activate   # On Windows
# source .venv/bin/activate  # On Linux/macOS

# 2. Install package and development dependencies
uv pip install -e ".[dev]"

# 3. Execute unit and integration tests
pytest tests/ -v
```

---

## 4. Usage Example

```python
import asyncio
from warden_cache import WardenCacheClient

async def main():
    # Connect via Redis Sentinel or standalone URL
    client = WardenCacheClient(redis_url="redis://localhost:6379")

    role = "Employee"
    query = "How many days of bereavement leave am I entitled to?"

    # 1. Check cache with XFetch probabilistic early refresh
    cached, should_refresh = await client.get(role, query)
    if cached and not should_refresh:
        print(f"Cache Hit: {cached.answer}")
        return

    # 2. On miss or early refresh: coordinate with SingleFlight
    worker_uuid = "pod-orchestrator-1"
    if await client.singleflight.acquire_lock(role, query, worker_uuid):
        try:
            # Winner executes backend hybrid retrieval + LLM generation
            answer = "Employees are eligible for up to 5 consecutive paid days off."
            await client.set(role, query, answer, citations=[], delta_t=0.68)
            await client.singleflight.notify_waiters(role, query, answer)
        finally:
            await client.singleflight.release_lock(role, query, worker_uuid)
    else:
        # Losers await computation result via pub/sub channel
        result = await client.singleflight.wait_for_result(role, query)
        print(f"Received computed result: {result}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 5. Verification & Test Suite Summary

The automated test suite covers all units and integration paths:

| Suite | Tests | Description |
|---|---|---|
| `test_k8s_manifests.py` | 3 | Validates Redis 512MB LRU, AOF persistence, Sentinel spec |
| `test_network_policy.py` | 1 | Verifies port 6379 restricted to orchestrator, retrieval, ingestion |
| `test_cache_keys.py` | 5 | Tests normalization, role partitioning, SHA-256 formatting |
| `test_xfetch.py` | 4 | Tests mathematical XFetch formula, clamping, CacheEnvelope |
| `test_singleflight.py` | 7 | Tests SET NX PX, atomic Lua unlock, Pub/Sub waiting channels |
| `test_invalidation.py` | 5 | Tests non-blocking SCAN + UNLINK and malformed payload safety |
| `test_client.py` | 5 | Tests unified client, Sentinel config, and fail-open resilience |
| `test_redis_integration.py`| 4 | 50-worker thundering herd simulation, failover, live lifecycle |
| **Total** | **34** | **100% Passing** |
