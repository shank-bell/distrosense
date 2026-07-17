from simulator.services.base_service import BaseService, ServiceConfig
from simulator.generators.metric_generator import generate_metrics

cfg = ServiceConfig(service_id="test-svc", name="test", tier="core", team="test")
svc = BaseService(cfg)

for i in range(30):
    m = generate_metrics(svc)
    print(f"[{i+1}] cpu={m['cpu_percent']:.2f} latency={m['latency_p99']:.2f} "
          f"error={m['error_rate']:.4f} req={m['request_rate']:.2f}")