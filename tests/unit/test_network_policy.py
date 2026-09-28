from pathlib import Path

import yaml


def test_isolate_redis_network_policy():
    netpol_path = Path("k8s/network-policies/isolate-redis.yaml")
    assert netpol_path.exists(), "isolate-redis.yaml missing"
    netpol = yaml.safe_load(netpol_path.read_text())

    assert netpol["metadata"]["name"] == "isolate-redis-cache"
    assert netpol["metadata"]["namespace"] == "warden-storage"
    assert set(netpol["spec"]["policyTypes"]) == {"Ingress", "Egress"}
    assert netpol["spec"]["egress"] == []

    ingress_rule = netpol["spec"]["ingress"][0]
    assert ingress_rule["ports"][0]["port"] == 6379

    # Verify permitted apps: warden-orchestrator, warden-retrieval, warden-ingestion
    permitted_apps = ingress_rule["from"][0]["podSelector"]["matchExpressions"][0]["values"]
    assert set(permitted_apps) == {"warden-orchestrator", "warden-retrieval", "warden-ingestion"}
