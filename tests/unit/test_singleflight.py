import pytest
from unittest.mock import AsyncMock, MagicMock
from warden_cache.singleflight import SingleFlightCoordinator

@pytest.mark.asyncio
async def test_acquire_lock_success():
    mock_redis = AsyncMock()
    mock_redis.set.return_value = True
    coord = SingleFlightCoordinator(mock_redis)

    acquired = await coord.acquire_lock("Employee", "PTO", "worker-1", ttl_ms=5000)
    assert acquired is True
    mock_redis.set.assert_awaited_once()
    args, kwargs = mock_redis.set.await_args
    assert kwargs.get("nx") is True
    assert kwargs.get("px") == 5000

@pytest.mark.asyncio
async def test_acquire_lock_contended():
    mock_redis = AsyncMock()
    mock_redis.set.return_value = None  # Key already exists
    coord = SingleFlightCoordinator(mock_redis)

    acquired = await coord.acquire_lock("Employee", "PTO", "worker-2", ttl_ms=5000)
    assert acquired is False

@pytest.mark.asyncio
async def test_atomic_release_lock_lua_script():
    # Review Focus 2: Atomic Lua script checks worker_uuid before deleting
    mock_redis = AsyncMock()
    mock_redis.eval.return_value = 1
    coord = SingleFlightCoordinator(mock_redis)

    released = await coord.release_lock("Employee", "PTO", "worker-1")
    assert released is True
    mock_redis.eval.assert_awaited_once()

@pytest.mark.asyncio
async def test_atomic_release_lock_wrong_uuid():
    mock_redis = AsyncMock()
    mock_redis.eval.return_value = 0  # Lock held by different worker or expired
    coord = SingleFlightCoordinator(mock_redis)

    released = await coord.release_lock("Employee", "PTO", "worker-1")
    assert released is False

@pytest.mark.asyncio
async def test_notify_waiters():
    mock_redis = AsyncMock()
    mock_redis.publish.return_value = 3  # 3 listeners received
    coord = SingleFlightCoordinator(mock_redis)

    listeners = await coord.notify_waiters("Employee", "PTO", "18 days.")
    assert listeners == 3
    mock_redis.publish.assert_awaited_once()

@pytest.mark.asyncio
async def test_wait_for_result_receives_message():
    mock_redis = AsyncMock()
    mock_pubsub = AsyncMock()
    # In redis-py, pubsub() is a synchronous method returning the PubSub object
    mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
    mock_redis.get.return_value = None  # Not in cache yet
    mock_pubsub.get_message.return_value = {
        "type": "message",
        "data": '{"answer": "published result"}',
    }

    coord = SingleFlightCoordinator(mock_redis)
    res = await coord.wait_for_result("Employee", "PTO", timeout_sec=1.0)
    assert res == '{"answer": "published result"}'

@pytest.mark.asyncio
async def test_wait_for_result_fallback_check():
    # Review Focus 3: Waiter checks cache on timeout before returning None
    mock_redis = AsyncMock()
    mock_pubsub = AsyncMock()
    mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
    # Simulate no message received on pubsub
    mock_pubsub.get_message.return_value = None
    # Immediate check returns None, fallback on timeout returns cached value
    mock_redis.get.side_effect = [None, '{"answer": "cached result"}']

    coord = SingleFlightCoordinator(mock_redis)
    res = await coord.wait_for_result("Employee", "PTO", timeout_sec=0.1)
    assert res == '{"answer": "cached result"}'
