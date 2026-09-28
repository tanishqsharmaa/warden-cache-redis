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
| `test_cache_keys.py` | 7 | Tests normalization (NFKC, ligatures), role partitioning, SHA-256 |
| `test_xfetch.py` | 4 | Tests mathematical XFetch formula, boundary clamping, CacheEnvelope |
| `test_singleflight.py` | 7 | Tests SET NX PX, atomic Lua unlock, Pub/Sub waiting channels |
| `test_invalidation.py` | 6 | Tests non-blocking SCAN + UNLINK, loop lifecycle, malformed payload safety |
| `test_client.py` | 8 | Tests unified client, Sentinel config, lifecycle, and fail-open resilience |
| `test_redis_integration.py`| 4 | 50-worker thundering herd simulation, failover, live lifecycle |
| **Total** | **40** | **100% Passing** |

---

## 6. Session Handoff & Platform Engineering Context

### 6.1 Status & Delivery State
- **Tier Classification**: Tier 1 (`warden-cache-redis`) — **100% COMPLETE & HARDENED**
- **Test Suite**: 40 passed (0 failures, 0 skipped) across unit and integration suites in 1.42s.
- **Static Analysis**: Zero lint errors (`ruff check`), strict type checking passed across all source files (`mypy --strict`).
- **Workspace Hygiene**: Clean working tree. Transient caches, build outputs, `.superpowers`, and documentation are guarded by `.gitignore`.
- **Master Build Sequence Gate**: Satisfies the caching half of **GATE-1** (`BUILD_SEQUENCE.md` § 4).

### 6.2 Key Architectural Decisions & Invariants
1. **MANDATE-01 (Database-Per-Service Isolation)**: Persistence boundaries are absolute. Redis operates purely as an ephemeral L2 cache and synchronization engine. It holds zero permanent document metadata or primary vector indices.
2. **MANDATE-03 (Early-Binding ACL Key Namespacing)**: `format_query_cache_key` strictly enforces `cache:query:{role_tier}:{sha256(normalize(query_text))}`. An `Employee` request can mathematically never hit a `Manager` or `HR-Admin` cache entry.
3. **Thundering Herd Elimination (XFetch & SingleFlight)**:
   - Early background refresh is governed by $\Delta = -\beta \cdot \delta \cdot \ln(U)$, where $U \in [10^{-10}, 1.0]$ and $\delta \ge 0.0$. Trigger rule: $(now + \Delta) > expiry$.
   - Cache misses synchronize via distributed mutex (`SET ... NX PX 5000`) and release atomically using Lua script `UNLOCK_LUA_SCRIPT` to prevent lock-stealing race conditions.
4. **Non-Blocking Cache Invalidation**: Subscribes to Redis topic `warden:cache:invalidate` and executes non-blocking `SCAN` in batches of 100 with asynchronous `UNLINK`, preventing single-threaded Redis event loop freezes.
5. **Fail-Open Resilience**: On Redis connectivity or timeout errors, `WardenCacheClient` logs operational warnings, increments `cache_error_count`, and gracefully returns `(None, False)` so downstream services fall back to live retrieval without dropping requests.

### 6.3 Downstream Consumption & Integration Points
1. **`warden-orchestrator` (Tier 5)**:
   - Queries `WardenCacheClient.get(role, query)`. On cache hit, returns answer within $<15\text{ms}$ (SLA: $<1.50\text{s}$).
   - On cache miss, acquires SingleFlight mutex, computes answer via retrieval and Azure OpenAI, stores envelope via `client.set()`, and notifies waiters.
2. **`warden-retrieval` (Tier 3)**:
   - Interfaces with `SingleFlightCoordinator` to coordinate speculative reranking or cached candidate sets.
3. **`warden-ingestion` (Tier 2)**:
   - Publishes `{"event": "INGESTION_COMPLETED", "run_id": "...", "affected_roles": ["Employee", "Manager"]}` to `warden:cache:invalidate` upon completing batch indexing.

### 6.4 Verification Quickstart for Incoming Engineers
```bash
# 1. Run complete automated test suite
.venv\Scripts\pytest.exe tests/ -v

# 2. Run static analysis and linting
.venv\Scripts\ruff.exe check src/ tests/
.venv\Scripts\mypy.exe src/

# 3. Verify Kubernetes manifests syntax
kubectl apply --dry-run=client -f k8s/redis/
kubectl apply --dry-run=client -f k8s/network-policies/
```

### 6.5 Next Build Phase (Tier 2: Document Ingestion Subsystem)
Per `Docs/BUILD_SEQUENCE.md`, with Tier 0 (`warden-shared`) and Tier 1 (`warden-infra`, `warden-cache-redis`) complete:
- **Next Target**: **Tier 2 (`warden-ingestion`)**
  - Scope: SQLite WAL idempotency ledger manager (`ingestion.db`), multiprocess Presidio PII scrubbing pool (`ProcessPoolExecutor`, 4 workers), table-aware recursive 512-token chunking with bounded queue (`asyncio.Queue(maxsize=256)`), batched INT8 ONNX Runtime vectorizer for `BAAI/bge-base-en-v1.5`, streaming gRPC client for `RetrievalService.IndexBatch`, and Redis invalidation publishing to `warden:cache:invalidate`.
  - Integration Gate: **GATE-2** (`pytest tests/integration/test_ingestion_pipeline.py -m "pii and wal"`).
