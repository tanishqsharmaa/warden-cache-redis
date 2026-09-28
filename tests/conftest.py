"""Pytest shared test fixtures for warden-cache-redis."""
import os
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
import fakeredis.aioredis as fake_aioredis


@pytest_asyncio.fixture
async def redis_client():
    """Provide an asynchronous Redis client for integration testing.

    Attempts to connect to live Redis if running at REDIS_URL (or localhost:6379).
    Falls back to in-process async FakeRedis with Lua scripting support if live Redis
    is unavailable in the test execution environment.
    """
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    real_client = aioredis.from_url(
        redis_url,
        socket_timeout=0.2,
        socket_connect_timeout=0.2,
        decode_responses=True,
    )
    
    is_live = False
    try:
        await real_client.ping()
        is_live = True
        client = real_client
    except Exception:
        await real_client.aclose()
        # Fallback to FakeRedis with Lua engine
        client = fake_aioredis.FakeRedis(decode_responses=True)

    try:
        yield client
    finally:
        try:
            await client.flushall()
        except Exception:
            pass
        await client.aclose()
