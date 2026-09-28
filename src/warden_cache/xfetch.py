"""XFetch Probabilistic Early Expiration Algorithm and Cache Envelope.

Denormalized from ARCHITECTURE_SPECIFICATION.md § 3.5.2 and Docs/warden-cache-redis.md § 3.4 & § 3.5.
"""
import math
import random
import time
from typing import Any

from pydantic import BaseModel, Field

from .keys import VALID_ROLE_TIERS


def compute_xfetch_delta(
    delta_t: float,
    beta: float = 1.0,
    u_rand: float | None = None,
) -> float:
    """Compute XFetch probabilistic expiration margin: Delta = -beta * delta_t * ln(U).

    Args:
        delta_t: Execution compute latency in seconds recorded at write time.
        beta: Aggressiveness tuning factor (default: 1.0).
        u_rand: Optional uniform random value in (0, 1]. Defaults to random.uniform(0, 1).

    Returns:
        Delta in seconds. If delta_t <= 0, returns 0.0.
    """
    if delta_t <= 0.0:
        return 0.0

    if u_rand is None:
        u_rand = random.uniform(0.0, 1.0)

    # Review Focus 1: Clamp U in [1e-10, 1.0] to prevent math domain error with ln(0)
    u_clamped = max(1e-10, min(1.0, float(u_rand)))
    delta = -float(beta) * float(delta_t) * math.log(u_clamped)
    return max(0.0, delta)


def should_refresh_early(
    expiry_epoch: float,
    delta_t: float,
    beta: float = 1.0,
    current_epoch: float | None = None,
) -> bool:
    """Evaluate XFetch early expiration decision rule: (now + Delta) > expiry_epoch.

    Denormalized from ARCHITECTURE_SPECIFICATION.md § 1.4 line 188:
    Delta = -beta * delta_t * ln(U) > (expiry - now) <=> (now + Delta) > expiry.

    As key approaches expiry, the probability of early background refresh asymptotically
    approaches 1.0. Exactly one worker triggers refresh before TTL expires.

    Args:
        expiry_epoch: Unix epoch timestamp when cache key expires.
        delta_t: Time taken to compute cached result in seconds.
        beta: Aggressiveness tuning factor (default: 1.0).
        current_epoch: Optional current Unix timestamp (default: time.time()).

    Returns:
        True if the item should be refreshed early; False otherwise.
    """
    now = time.time() if current_epoch is None else float(current_epoch)
    if now >= expiry_epoch:
        return True

    delta = compute_xfetch_delta(delta_t, beta=beta)
    return (now + delta) > expiry_epoch


class CacheEnvelope(BaseModel):
    """Envelope metadata stored inside Redis L2 cache with cached answer and citations."""

    query: str = Field(..., description="Original raw or normalized query text")
    caller_role: str = Field(..., description="Access control security role tier")
    answer: str = Field(..., description="Generated answer text or summarized response")
    citations: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of citation references with doc_id, chunk_index, source_url",
    )
    delta_t_seconds: float = Field(
        ...,
        description="Compute time in seconds required to produce the answer",
    )
    written_at_epoch: float = Field(
        ...,
        description="Unix epoch timestamp when this entry was written to cache",
    )
    expiry_epoch: float = Field(
        ...,
        description="Unix epoch timestamp when this entry expires in Redis",
    )
