import yaml
from pathlib import Path

def test_redis_configmap_directives():
    config_path = Path("k8s/redis/redis-configmap.yaml")
    assert config_path.exists(), "redis-configmap.yaml missing"
    docs = list(yaml.safe_load_all(config_path.read_text()))
    cm = next(d for d in docs if d["kind"] == "ConfigMap")
    data = cm["data"]["redis.conf"]
    assert "maxmemory 512mb" in data
    assert "maxmemory-policy allkeys-lru" in data
    assert "appendonly yes" in data
    assert "appendfsync everysec" in data

def test_redis_statefulset_spec():
    sts_path = Path("k8s/redis/redis-statefulset.yaml")
    assert sts_path.exists(), "redis-statefulset.yaml missing"
    docs = list(yaml.safe_load_all(sts_path.read_text()))
    sts = next(d for d in docs if d["kind"] == "StatefulSet")
    assert sts["spec"]["serviceName"] == "redis-headless"
    container = sts["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "redis:7.2.4-alpine"
    pvc = sts["spec"]["volumeClaimTemplates"][0]
    assert pvc["spec"]["resources"]["requests"]["storage"] == "5Gi"

def test_sentinel_deployment_spec():
    sentinel_path = Path("k8s/redis/sentinel-deployment.yaml")
    assert sentinel_path.exists(), "sentinel-deployment.yaml missing"
    docs = list(yaml.safe_load_all(sentinel_path.read_text()))
    dep = next(d for d in docs if d["kind"] == "Deployment")
    assert dep["spec"]["replicas"] == 3
