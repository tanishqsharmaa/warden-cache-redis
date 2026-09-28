import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from warden_cache.invalidation import CacheInvalidationSubscriber


@pytest.mark.asyncio
async def test_purge_role_keys_uses_scan_and_unlink():
    mock_redis = AsyncMock()
    # Mock SCAN returning cursor 0 and 2 keys
    mock_redis.scan.side_effect = [(0, ["cache:query:Employee:abc", "cache:query:Employee:def"])]
    mock_redis.unlink.return_value = 2

    sub = CacheInvalidationSubscriber(mock_redis)
    count = await sub.purge_role_keys("Employee", batch_size=100)
    assert count == 2
    mock_redis.scan.assert_awaited_once_with(cursor=0, match="cache:query:Employee:*", count=100)
    mock_redis.unlink.assert_awaited_once_with(
        "cache:query:Employee:abc", "cache:query:Employee:def"
    )


@pytest.mark.asyncio
async def test_purge_role_keys_multi_batch():
    mock_redis = AsyncMock()
    # Mock multi-batch SCAN (cursor 42 -> cursor 0)
    mock_redis.scan.side_effect = [
        (42, ["cache:query:Manager:1", "cache:query:Manager:2"]),
        (0, ["cache:query:Manager:3"]),
    ]
    mock_redis.unlink.side_effect = [2, 1]

    sub = CacheInvalidationSubscriber(mock_redis)
    count = await sub.purge_role_keys("Manager", batch_size=100)
    assert count == 3
    assert mock_redis.scan.await_count == 2
    assert mock_redis.unlink.await_count == 2


@pytest.mark.asyncio
async def test_purge_role_keys_no_keys_found():
    mock_redis = AsyncMock()
    mock_redis.scan.return_value = (0, [])

    sub = CacheInvalidationSubscriber(mock_redis)
    count = await sub.purge_role_keys("HR-Admin", batch_size=100)
    assert count == 0
    mock_redis.unlink.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_invalidation_payload_valid():
    mock_redis = AsyncMock()
    mock_redis.scan.return_value = (0, [])
    sub = CacheInvalidationSubscriber(mock_redis)

    payload = json.dumps(
        {
            "event": "INGESTION_COMPLETED",
            "run_id": "run-001",
            "affected_roles": ["Employee", "Manager"],
            "timestamp": "2026-09-27T10:01:36.400Z",
        }
    )
    roles = await sub.handle_invalidation_payload(payload)
    assert roles == ["Employee", "Manager"]


@pytest.mark.asyncio
async def test_handle_invalidation_payload_malformed_ignored():
    # Review Focus 5: Malformed JSON or unknown roles ignored safely
    mock_redis = AsyncMock()
    sub = CacheInvalidationSubscriber(mock_redis)

    # Completely unparseable JSON
    res_bad_json = await sub.handle_invalidation_payload("NOT_JSON")
    assert res_bad_json == []

    # Valid JSON but missing affected_roles
    res_missing_roles = await sub.handle_invalidation_payload('{"event": "INGESTION_COMPLETED"}')
    assert res_missing_roles == []

    # Valid JSON with invalid role outside VALID_ROLE_TIERS
    res_invalid_role = await sub.handle_invalidation_payload(
        '{"event": "INGESTION_COMPLETED", "affected_roles": ["InvalidRole"]}'
    )
    assert res_invalid_role == []


@pytest.mark.asyncio
async def test_start_subscription_loop_clean_exit():
    from unittest.mock import MagicMock

    mock_redis = AsyncMock()
    mock_pubsub = AsyncMock()
    mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
    mock_pubsub.get_message.return_value = None

    sub = CacheInvalidationSubscriber(mock_redis)
    stop_event = asyncio.Event()

    # Trigger stop event after short delay
    async def trigger_stop():
        await asyncio.sleep(0.05)
        stop_event.set()

    await asyncio.gather(
        sub.start_subscription_loop(stop_event),
        trigger_stop(),
    )

    mock_pubsub.subscribe.assert_awaited_once_with("warden:cache:invalidate")
    mock_pubsub.unsubscribe.assert_awaited_once_with("warden:cache:invalidate")
