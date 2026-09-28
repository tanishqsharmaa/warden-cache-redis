"""Distributed SingleFlight Mutex Lock and Pub/Sub Coordination.

Denormalized from ARCHITECTURE_SPECIFICATION.md § 3.5.3 and Docs/warden-cache-redis.md § 3.4.
"""
import asyncio
import logging
import time

import redis.asyncio as aioredis

from .keys import format_lock_key, format_pubsub_channel, format_query_cache_key

logger = logging.getLogger(__name__)

# Atomic release script: only delete the lock key if the value matches the caller's worker_uuid
UNLOCK_LUA_SCRIPT: str = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class SingleFlightCoordinator:
    """Orchestrates distributed mutual exclusion locks and completion pub/sub notifications."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self.redis = redis_client

    async def acquire_lock(
        self,
        role_tier: str,
        query: str,
        worker_uuid: str,
        ttl_ms: int = 5000,
    ) -> bool:
        """Attempt atomic distributed lock acquisition via SET NX PX.

        Args:
            role_tier: Security role tier ('Employee', 'Manager', 'HR-Admin').
            query: Query text.
            worker_uuid: Unique worker process/task identifier.
            ttl_ms: Lock expiration in milliseconds (default: 5000ms).

        Returns:
            True if lock was acquired; False if already held by another worker.
        """
        lock_key = format_lock_key(role_tier, query)
        res = await self.redis.set(lock_key, worker_uuid, nx=True, px=ttl_ms)
        return bool(res)

    async def release_lock(
        self,
        role_tier: str,
        query: str,
        worker_uuid: str,
    ) -> bool:
        """Atomically release the distributed lock if held by this worker_uuid.

        Review Focus 2: Atomic Lua script prevents deleting a newly acquired lock if the
        previous holder took longer than ttl_ms.

        Args:
            role_tier: Security role tier.
            query: Query text.
            worker_uuid: Unique worker identifier that acquired the lock.

        Returns:
            True if the lock was successfully released; False otherwise.
        """
        lock_key = format_lock_key(role_tier, query)
        res = await self.redis.eval(UNLOCK_LUA_SCRIPT, 1, lock_key, worker_uuid)
        return bool(res)

    async def notify_waiters(
        self,
        role_tier: str,
        query: str,
        payload: str,
    ) -> int:
        """Publish the computed result to contending waiting workers.

        Args:
            role_tier: Security role tier.
            query: Query text.
            payload: Serialized JSON result string.

        Returns:
            Count of active subscribers that received the message.
        """
        chan = format_pubsub_channel(role_tier, query)
        res = await self.redis.publish(chan, payload)
        return int(res)

    async def wait_for_result(
        self,
        role_tier: str,
        query: str,
        timeout_sec: float = 3.5,
    ) -> str | None:
        """Subscribe to completion channel and wait for computation result.

        Review Focus 3: Waiter checks cache on subscription or timeout to handle
        pub/sub race condition where winner completed before subscriber finished connecting.

        Args:
            role_tier: Security role tier.
            query: Query text.
            timeout_sec: Maximum wait duration before falling back to cache (default: 3.5s).

        Returns:
            Cached result string if received, or None on failure/unresolved.
        """
        chan = format_pubsub_channel(role_tier, query)
        cache_key = format_query_cache_key(role_tier, query)
        pubsub = self.redis.pubsub()

        try:
            await pubsub.subscribe(chan)

            # Check cache immediately after subscribing in case computation just finished
            immediate_check = await self.redis.get(cache_key)
            if immediate_check is not None:
                return str(immediate_check)

            start_time = time.monotonic()
            while (time.monotonic() - start_time) < timeout_sec:
                remaining = max(0.01, timeout_sec - (time.monotonic() - start_time))
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(0.2, remaining),
                )
                if msg is not None and msg.get("type") == "message":
                    data = msg.get("data")
                    if data is not None:
                        return str(data)
                await asyncio.sleep(0.02)

            # Timeout reached — fallback to direct cache check before returning None
            fallback_val = await self.redis.get(cache_key)
            if fallback_val is not None:
                return str(fallback_val)
            return None

        except Exception as exc:
            logger.warning("Error waiting for SingleFlight result on channel '%s': %s", chan, exc)
            # Final fallback check on error
            try:
                fallback_val = await self.redis.get(cache_key)
                if fallback_val is not None:
                    return str(fallback_val)
            except Exception:
                pass
            return None

        finally:
            try:
                await pubsub.unsubscribe(chan)
                await pubsub.aclose()  # type: ignore[no-untyped-call]
            except Exception:
                pass
