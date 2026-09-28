import pytest
from warden_cache.keys import (
    normalize_query,
    format_query_cache_key,
    format_lock_key,
    format_pubsub_channel,
    VALID_ROLE_TIERS,
)

def test_query_normalization_whitespace_and_unicode():
    raw = "   How \t much \n PTO do I get?   "
    normalized = normalize_query(raw)
    assert normalized == "how much pto do i get?"

def test_format_query_cache_key_valid_roles():
    key_emp = format_query_cache_key("Employee", "How much PTO?")
    assert key_emp.startswith("cache:query:Employee:")
    assert len(key_emp.split(":")[-1]) == 64  # SHA-256 length

    key_mgr = format_query_cache_key("Manager", "Performance review")
    assert key_mgr.startswith("cache:query:Manager:")

    key_admin = format_query_cache_key("HR-Admin", "Severance policy")
    assert key_admin.startswith("cache:query:HR-Admin:")

def test_format_query_cache_key_invalid_role_raises():
    with pytest.raises(ValueError, match="Invalid role_tier"):
        format_query_cache_key("Contractor", "How much PTO?")

    with pytest.raises(ValueError, match="Invalid role_tier"):
        format_query_cache_key("Admin", "How much PTO?")

def test_lock_key_and_channel_formatting():
    lock_key = format_lock_key("Manager", "Bonus structure")
    assert lock_key.startswith("lock:query:Manager:")
    assert len(lock_key.split(":")[-1]) == 64

    chan = format_pubsub_channel("HR-Admin", "Severance")
    assert chan.startswith("channel:query:HR-Admin:")
    assert len(chan.split(":")[-1]) == 64

def test_valid_role_tiers_set():
    assert VALID_ROLE_TIERS == frozenset({"Employee", "Manager", "HR-Admin"})
