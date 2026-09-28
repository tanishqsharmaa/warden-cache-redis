"""Warden Distributed Redis Cache Client with Sentinel HA, XFetch, and SingleFlight.

Denormalized from ARCHITECTURE_SPECIFICATION.md § 3.5, § 4.3.1 and Docs/warden-cache-redis.md § 3.3.
"""
import asyncio
import logging
import time
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

from .invalidation import CacheInvalidationSubscriber
from .keys import format_query_cache_key
from .singleflight import SingleFlightCoordinator
from .xfetch import CacheEnvelope, should_refresh_early

logger = logging.getLogger(__name__)


class WardenCacheClient:
    """High-performance async Redis cache client for Project Warden.

    Features:
    - Sentinel High Availability and RESP3 protocol standard
    - Role-partitioned cache key security
    - XFetch probabilistic early expiration to prevent thundering herds
    - SingleFlight distributed mutex locks and pub/sub coordination
    - Non-blocking SCAN + UNLINK cache invalidation
    - Fail-open resilience: graceful degradation to live retrieval on Redis error
    """

    def __init__(
        self,
        redis_client: aioredis.Redis | None = None,
        redis_url: str = "redis://warden-cache-redis:6379",
        sentinel_hosts: list[tuple[str, int]] | None = None,
        service_name: str = "mymaster",
        max_connections: int = 50,
        socket_timeout: float = 0.2,
        socket_connect_timeout: float = 0.5,
    ) -> None:
        self.redis_url = redis_url
        self.sentinel_hosts = sentinel_hosts
        self.service_name = service_name
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.cache_error_count: int = 0
        self._lock = asyncio.Lock()

        if redis_client is not None:
            self._redis = redis_client
            self._own_client = False
        else:
            self._redis = None  # type: ignore[assignment]
            self._own_client = True

        self._singleflight: SingleFlightCoordinator | None = None
        self._invalidation: CacheInvalidationSubscriber | None = None

    async def get_raw_client(self) -> aioredis.Redis:
        """Acquire or lazily initialize the underlying aioredis.Redis connection."""
        if self._redis is not None:
            return self._redis

        async with self._lock:
            if self._redis is not None:
                return self._redis

            if self.sentinel_hosts:
                sentinel = Sentinel(
                    self.sentinel_hosts,
                    socket_timeout=self.socket_timeout,
                    socket_connect_timeout=self.socket_connect_timeout,
                )
                self._redis = sentinel.master_for(
                    self.service_name,
                    max_connections=self.max_connections,
                    socket_timeout=self.socket_timeout,
                    socket_connect_timeout=self.socket_connect_timeout,
                    decode_responses=True,
                    protocol=3,
                )
            else:
                self._redis = aioredis.from_url(
                    self.redis_url,
                    max_connections=self.max_connections,
                    socket_timeout=self.socket_timeout,
                    socket_connect_timeout=self.socket_connect_timeout,
                    decode_responses=True,
                    protocol=3,
                )

            return self._redis

    @property
    def singleflight(self) -> SingleFlightCoordinator:
        """Distributed SingleFlight mutex and coordination helper."""
        if self._redis is None:
            raise RuntimeError("Redis client not initialized; call get_raw_client() first.")
        if self._singleflight is None:
            self._singleflight = SingleFlightCoordinator(self._redis)
        return self._singleflight

    @property
    def invalidation(self) -> CacheInvalidationSubscriber:
        """Cache invalidation subscriber helper."""
        if self._redis is None:
            raise RuntimeError("Redis client not initialized; call get_raw_client() first.")
        if self._invalidation is None:
            self._invalidation = CacheInvalidationSubscriber(self._redis)
        return self._invalidation

    async def get(
        self,
        role: str,
        query: str,
        beta: float = 1.0,
    ) -> tuple[CacheEnvelope | None, bool]:
        """Retrieve cached query envelope and evaluate XFetch early expiration.

        Review Focus 4: Fails open gracefully on Redis connection or timeout error,
        returning (None, False) and incrementing cache_error_count.

        Args:
            role: Caller security role ('Employee', 'Manager', 'HR-Admin').
            query: Query text.
            beta: XFetch aggressiveness tuning factor (default: 1.0).

        Returns:
            Tuple of (envelope or None, should_refresh_early boolean).
        """
        try:
            client = await self.get_raw_client()
            cache_key = format_query_cache_key(role, query)
            raw_val = await client.get(cache_key)

            if raw_val is None:
                return None, False

            raw_str = raw_val.decode("utf-8") if isinstance(raw_val, bytes) else str(raw_val)
            envelope = CacheEnvelope.model_validate_json(raw_str)
            should_refresh = should_refresh_early(
                expiry_epoch=envelope.expiry_epoch,
                delta_t=envelope.delta_t_seconds,
                beta=beta,
            )
            return envelope, should_refresh

        except Exception as exc:
            self.cache_error_count += 1
            logger.warning(
                "Redis cache read error for role '%s' (failing open to live retrieval): %s",
                role,
                exc,
            )
            return None, False

    async def set(
        self,
        role: str,
        query: str,
        answer: str,
        citations: list[dict[str, Any]] | None = None,
        delta_t: float = 0.0,
        ttl_sec: int = 1800,
    ) -> None:
        """Store computed answer envelope in Redis L2 cache with TTL.

        Review Focus 4: Catches connection and timeout errors, logging a warning and
        incrementing cache_error_count without crashing the caller.

        Args:
            role: Caller security role tier.
            query: Original query text.
            answer: Generated or retrieved answer text.
            citations: List of document citations.
            delta_t: Execution compute time in seconds.
            ttl_sec: Time-to-live in seconds (default: 1800 / 30 mins).
        """
        now = time.time()
        envelope = CacheEnvelope(
            query=query,
            caller_role=role,
            answer=answer,
            citations=citations or [],
            delta_t_seconds=delta_t,
            written_at_epoch=now,
            expiry_epoch=now + ttl_sec,
        )

        try:
            client = await self.get_raw_client()
            cache_key = format_query_cache_key(role, query)
            await client.set(cache_key, envelope.model_dump_json(), ex=ttl_sec)
        except Exception as exc:
            self.cache_error_count += 1
            logger.warning(
                "Redis cache write error for key '%s': %s",
                format_query_cache_key(role, query) if role in ("Employee", "Manager", "HR-Admin") else "unknown",
                exc,
            )

    async def ping(self) -> bool:
        """Test Redis server connectivity."""
        try:
            client = await self.get_raw_client()
            res = await client.ping()
            return bool(res)
        except Exception as exc:
            self.cache_error_count += 1
            logger.warning("Redis ping failed: %s", exc)
            return False

    async def close(self) -> None:
        """Cleanly close underlying connection pool if owned by this client."""
        if self._own_client and self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                pass
            self._redis = None  # type: ignore[assignment]
