import time
from unittest.mock import AsyncMock

import pytest

from warden_cache.client import WardenCacheClient
from warden_cache.xfetch import CacheEnvelope


@pytest.mark.asyncio
async def test_client_get_cache_hit_and_miss():
    mock_redis = AsyncMock()
    # Cache hit
    env = CacheEnvelope(
        query="PTO",
        caller_role="Employee",
        answer="18 days",
        citations=[{"doc_id": "DOC-1", "chunk_index": 0}],
        delta_t_seconds=0.2,
        written_at_epoch=time.time(),
        expiry_epoch=time.time() + 1800,
    )
    mock_redis.get.return_value = env.model_dump_json()

    client = WardenCacheClient(redis_client=mock_redis)
    result, should_refresh = await client.get("Employee", "PTO")
    assert result is not None
    assert result.answer == "18 days"
    assert should_refresh is False


@pytest.mark.asyncio
async def test_client_get_cache_miss():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    client = WardenCacheClient(redis_client=mock_redis)
    result, should_refresh = await client.get("Employee", "Uncached query")
    assert result is None
    assert should_refresh is False


@pytest.mark.asyncio
async def test_client_set_serializes_envelope():
    mock_redis = AsyncMock()
    client = WardenCacheClient(redis_client=mock_redis)

    await client.set(
        role="Manager",
        query="Budget authority",
        answer="Up to $50,000.",
        citations=[{"doc_id": "DOC-FIN-2026", "chunk_index": 2}],
        delta_t=0.65,
        ttl_sec=1800,
    )

    mock_redis.set.assert_awaited_once()
    args, kwargs = mock_redis.set.await_args
    key = args[0]
    val_json = args[1]
    assert key.startswith("cache:query:Manager:")
    assert kwargs.get("ex") == 1800

    envelope = CacheEnvelope.model_validate_json(val_json)
    assert envelope.caller_role == "Manager"
    assert envelope.answer == "Up to $50,000."
    assert envelope.delta_t_seconds == 0.65


@pytest.mark.asyncio
async def test_client_fail_open_on_redis_error():
    # Review Focus 4: Fail open on Redis connection/timeout error
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = ConnectionError("Redis cluster unreachable")

    client = WardenCacheClient(redis_client=mock_redis)
    result, should_refresh = await client.get("Employee", "PTO")
    # Must fail open gracefully without raising
    assert result is None
    assert should_refresh is False


@pytest.mark.asyncio
async def test_client_set_fail_open_on_redis_error():
    mock_redis = AsyncMock()
    mock_redis.set.side_effect = TimeoutError("Redis socket timeout")

    client = WardenCacheClient(redis_client=mock_redis)
    # Should not raise exception
    await client.set(
        role="Employee",
        query="PTO",
        answer="18 days",
        delta_t=0.1,
    )
    assert client.cache_error_count == 1


@pytest.mark.asyncio
async def test_client_ping_failure():
    mock_redis = AsyncMock()
    mock_redis.ping.side_effect = ConnectionError("Redis down")

    client = WardenCacheClient(redis_client=mock_redis)
    assert await client.ping() is False
    assert client.cache_error_count == 1


@pytest.mark.asyncio
async def test_client_properties_uninitialized_raises():
    client = WardenCacheClient()
    with pytest.raises(RuntimeError, match="Redis client not initialized"):
        _ = client.singleflight

    with pytest.raises(RuntimeError, match="Redis client not initialized"):
        _ = client.invalidation


@pytest.mark.asyncio
async def test_client_close_owned_client():
    mock_redis = AsyncMock()
    client = WardenCacheClient()
    client._redis = mock_redis
    client._own_client = True

    await client.close()
    mock_redis.aclose.assert_awaited_once()
    assert client._redis is None
