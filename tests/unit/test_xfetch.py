import time
import pytest
from warden_cache.xfetch import compute_xfetch_delta, should_refresh_early, CacheEnvelope

def test_xfetch_delta_computation_deterministic():
    # If delta_t = 1.0, beta = 1.0, u_rand = 0.5: delta = -1.0 * 1.0 * ln(0.5) ≈ 0.693147
    delta = compute_xfetch_delta(delta_t=1.0, beta=1.0, u_rand=0.5)
    assert pytest.approx(delta, 0.001) == 0.6931

def test_xfetch_boundary_zero_and_negative_inputs():
    # Review Focus 1: U=0 clamp, delta_t <= 0 clamp
    delta_u_zero = compute_xfetch_delta(delta_t=1.0, beta=1.0, u_rand=0.0)
    assert delta_u_zero > 0.0 and delta_u_zero < 100.0  # Safe finite value, no exception

    delta_neg_t = compute_xfetch_delta(delta_t=-0.5, beta=1.0, u_rand=0.5)
    assert delta_neg_t == 0.0

    delta_zero_t = compute_xfetch_delta(delta_t=0.0, beta=1.0, u_rand=0.5)
    assert delta_zero_t == 0.0

def test_should_refresh_early_decision():
    now = time.time()
    # Cache key expiring in 100 seconds with 0.1s delta_t should NOT refresh early
    assert not should_refresh_early(expiry_epoch=now + 100, delta_t=0.1, current_epoch=now)

    # Cache key already past expiry should definitely refresh early
    assert should_refresh_early(expiry_epoch=now - 1, delta_t=0.1, current_epoch=now)

    # Near-expiry condition with large computation time triggers refresh early
    # (now - delta) > expiry where delta is huge
    assert should_refresh_early(expiry_epoch=now + 1, delta_t=10.0, beta=5.0, current_epoch=now)

def test_cache_envelope_serialization():
    env = CacheEnvelope(
        query="How much PTO?",
        caller_role="Employee",
        answer="18 days.",
        citations=[
            {
                "citation_id": 1,
                "doc_id": "DOC-HR-LEAVE-2026",
                "chunk_index": 4,
                "source_url": "file:///data/policies/pto_policy_2026.md",
                "quoted_snippet": "Full-time employees receive 18 days...",
            }
        ],
        delta_t_seconds=0.45,
        written_at_epoch=1790500800.0,
        expiry_epoch=1790502600.0,
    )
    dumped = env.model_dump_json()
    restored = CacheEnvelope.model_validate_json(dumped)
    assert restored.query == env.query
    assert restored.caller_role == env.caller_role
    assert restored.delta_t_seconds == 0.45
    assert len(restored.citations) == 1
    assert restored.citations[0]["doc_id"] == "DOC-HR-LEAVE-2026"
