import numpy as np
from ml_service.gateway.grpc_gateway import MLInferenceServicer
from ml_service.proto import ml_service_pb2
from ml_service.training.dataset import generate_synthetic_dataset


def run_e2e_test(n_steps: int = 300):
    print("[E2E] Loading gateway servicer (real trained model, threshold, norm stats)...")
    servicer = MLInferenceServicer()

    if servicer._transformer is None:
        print("[E2E] Transformer model not loaded — aborting test.")
        return

    print(f"[E2E] Generating {n_steps} sequential readings for one service, with injected anomalies...")
    services, masks = generate_synthetic_dataset(
        n_services=1, n_steps=n_steps, inject_anomalies=True, anomaly_duty_cycle=0.1
    )
    data, mask = services[0], masks[0]

    service_id = "e2e-test-service"
    model_used_counts = {}
    first_transformer_tick = None
    confirmed_alerts = []

    for i in range(n_steps):
        request = ml_service_pb2.InferenceRequest(
            service_id=service_id,
            feature_vector=data[i].tolist(),
            window_id=f"w{i}",
            span_id=f"s{i}",
            trace_id="e2e-trace",
        )
        result = servicer.Infer(request, context=None)

        model_used_counts[result.model_used] = model_used_counts.get(result.model_used, 0) + 1
        if result.model_used == "TRANSFORMER" and first_transformer_tick is None:
            first_transformer_tick = i

        if result.is_anomaly:
            confirmed_alerts.append((i, bool(mask[i]), result.reconstruction_error))

    print(f"[E2E] Model usage across {n_steps} ticks: {model_used_counts}")
    print(f"[E2E] Transformer first kicked in at tick {first_transformer_tick} "
          f"(expected: tick 59 — the 60th reading, since the buffer needs a full real window first)")
    print(f"[E2E] Confirmed (debounced) alerts: {len(confirmed_alerts)}")
    for tick, was_real_anomaly, err in confirmed_alerts[:10]:
        print(f"    tick={tick} real_anomaly_at_this_tick={was_real_anomaly} reconstruction_error={err:.4f}")
    if len(confirmed_alerts) > 10:
        print(f"    ... and {len(confirmed_alerts) - 10} more")


if __name__ == "__main__":
    run_e2e_test(n_steps=300)