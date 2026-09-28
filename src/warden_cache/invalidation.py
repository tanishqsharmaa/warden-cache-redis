"""Cache Invalidation Subscriber and Asynchronous SCAN/UNLINK Module.

Denormalized from ARCHITECTURE_SPECIFICATION.md § 3.5.4 and Docs/warden-cache-redis.md § 3.6.
"""
import asyncio
import json
import logging
from typing import Any

import redis.asyncio as aioredis

from .keys import VALID_ROLE_TIERS

logger = logging.getLogger(__name__)

INVALIDATION_TOPIC: str = "warden:cache:invalidate"


class CacheInvalidationSubscriber:
    """Subscribes to cache invalidation topic and purges role-partitioned keys asynchronously."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        topic: str = INVALIDATION_TOPIC,
    ) -> None:
        self.redis = redis_client
        self.topic = topic

    async def purge_role_keys(self, role: str, batch_size: int = 100) -> int:
        """Purge all cache keys matching cache:query:{role}:* using non-blocking SCAN + UNLINK.

        Args:
            role: Security role tier to purge ('Employee', 'Manager', 'HR-Admin').
            batch_size: Redis SCAN batch cursor size (default: 100).

        Returns:
            Total count of keys asynchronously unlinked.
        """
        if role not in VALID_ROLE_TIERS:
            logger.warning("Attempted to purge keys for invalid role tier '%s'", role)
            return 0

        pattern = f"cache:query:{role}:*"
        cursor = 0
        total_unlinked = 0

        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match=pattern, count=batch_size)
            if keys:
                # UNLINK is asynchronous and non-blocking in Redis 4.0+
                unlinked = await self.redis.unlink(*keys)
                total_unlinked += int(unlinked)
            if cursor == 0:
                break

        logger.info("Purged %d cache keys matching pattern '%s'", total_unlinked, pattern)
        return total_unlinked

    async def handle_invalidation_payload(self, payload_str: str) -> list[str]:
        """Parse invalidation event and trigger UNLINK on affected roles.

        Review Focus 5: Safely ignores malformed JSON or unknown roles without crashing.

        Args:
            payload_str: Raw string received from Redis Pub/Sub topic.

        Returns:
            List of valid role tiers that were purged.
        """
        try:
            data = json.loads(payload_str)
        except Exception as exc:
            logger.warning("Received unparseable JSON on topic '%s': %s (data: %r)", self.topic, exc, payload_str)
            return []

        if not isinstance(data, dict):
            logger.warning("Invalid payload structure on topic '%s' (expected dict): %r", self.topic, data)
            return []

        raw_roles = data.get("affected_roles")
        if not isinstance(raw_roles, list) or not raw_roles:
            logger.warning("Payload on topic '%s' missing or has empty 'affected_roles': %r", self.topic, data)
            return []

        valid_roles_to_purge = [r for r in raw_roles if isinstance(r, str) and r in VALID_ROLE_TIERS]
        if not valid_roles_to_purge:
            logger.warning("No valid roles found in affected_roles: %r", raw_roles)
            return []

        purged_roles: list[str] = []
        for role in valid_roles_to_purge:
            await self.purge_role_keys(role)
            purged_roles.append(role)

        logger.info(
            "Successfully processed invalidation event run_id='%s' for roles %s",
            data.get("run_id", "UNKNOWN"),
            purged_roles,
        )
        return purged_roles

    async def start_subscription_loop(self, stop_event: asyncio.Event) -> None:
        """Run long-lived background listener for cache invalidation events.

        Args:
            stop_event: asyncio.Event used to signal clean shutdown.
        """
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.topic)
        logger.info("Subscribed to cache invalidation topic '%s'", self.topic)

        try:
            while not stop_event.is_set():
                try:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=0.5,
                    )
                    if msg is not None and msg.get("type") == "message":
                        data = msg.get("data")
                        if isinstance(data, (str, bytes)):
                            text_data = data.decode("utf-8") if isinstance(data, bytes) else data
                            await self.handle_invalidation_payload(text_data)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error("Error in invalidation subscription loop: %s", exc)
                    await asyncio.sleep(0.5)
        finally:
            try:
                await pubsub.unsubscribe(self.topic)
                await pubsub.aclose()
            except Exception:
                pass
            logger.info("Unsubscribed from cache invalidation topic '%s'", self.topic)
