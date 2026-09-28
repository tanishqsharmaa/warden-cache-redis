"""Cache Key Generation and Query Normalization Utilities.

Denormalized from ARCHITECTURE_SPECIFICATION.md § 3.5.1 and Docs/warden-cache-redis.md § 3.4.
"""

import hashlib
import re
import unicodedata

VALID_ROLE_TIERS: frozenset[str] = frozenset({"Employee", "Manager", "HR-Admin"})


def normalize_query(query_text: str) -> str:
    """Normalize query text for deterministic hashing.

    Applies NFKC unicode normalization, strips leading/trailing whitespace,
    lowercases all characters, and collapses repeated whitespace runs into single spaces.
    """
    normalized = unicodedata.normalize("NFKC", query_text.strip())
    lowered = normalized.lower()
    collapsed = re.sub(r"\s+", " ", lowered)
    return collapsed


def _compute_query_hash(query_text: str) -> str:
    """Compute SHA-256 hex digest of the normalized query text."""
    normalized = normalize_query(query_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def format_query_cache_key(role_tier: str, query_text: str) -> str:
    """Format role-partitioned cache key: cache:query:{role_tier}:{sha256(normalize(query_text))}."""
    if role_tier not in VALID_ROLE_TIERS:
        raise ValueError(
            f"Invalid role_tier '{role_tier}'. Must be one of {sorted(VALID_ROLE_TIERS)}."
        )
    q_hash = _compute_query_hash(query_text)
    return f"cache:query:{role_tier}:{q_hash}"


def format_lock_key(role_tier: str, query_text: str) -> str:
    """Format distributed SingleFlight mutex lock key: lock:query:{role_tier}:{hash}."""
    if role_tier not in VALID_ROLE_TIERS:
        raise ValueError(
            f"Invalid role_tier '{role_tier}'. Must be one of {sorted(VALID_ROLE_TIERS)}."
        )
    q_hash = _compute_query_hash(query_text)
    return f"lock:query:{role_tier}:{q_hash}"


def format_pubsub_channel(role_tier: str, query_text: str) -> str:
    """Format SingleFlight completion notification channel: channel:query:{role_tier}:{hash}."""
    if role_tier not in VALID_ROLE_TIERS:
        raise ValueError(
            f"Invalid role_tier '{role_tier}'. Must be one of {sorted(VALID_ROLE_TIERS)}."
        )
    q_hash = _compute_query_hash(query_text)
    return f"channel:query:{role_tier}:{q_hash}"
