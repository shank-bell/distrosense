import numpy as np
from ml_service.gateway.grpc_gateway import MLInferenceServicer
from ml_service.proto import ml_service_pb2
from ml_service.training.dataset import generate_synthetic_dataset
from ml_service.config import TRANSFORMER_SEQUENCE_LEN


def run_e2e_test(n_steps: int = 500, anomaly_duty_cycle: float = 0.05):
    print("[E2E] Loading gateway servicer (real trained model, threshold, norm stats, iforest)...")
    servicer = MLInferenceServicer()

    if servicer._transformer is None:
        print("[E2E] Transformer model not loaded — aborting test.")
        return
    if servicer._iforest is None:
        print(f"[E2E] WARNING: Isolation Forest not loaded — warmup period "
              f"(first {TRANSFORMER_SEQUENCE_LEN} ticks) will have zero anomaly coverage.")

    print(f"[E2E] Generating {n_steps} sequential readings, anomaly_duty_cycle={anomaly_duty_cycle} "
          f"(matches the real eval config, not an inflated test value)...")
    services, masks = generate_synthetic_dataset(
        n_services=1, n_steps=n_steps, inject_anomalies=True, anomaly_duty_cycle=anomaly_duty_cycle
    )
    data, mask = services[0], masks[0]
    seq_len = TRANSFORMER_SEQUENCE_LEN

    service_id = "e2e-test-service"
    model_used_counts = {}
    first_transformer_tick = None
    confirmed_alerts = []
    tp = fp = fn = tn = 0

    for i in range(n_steps):
        request = ml_service_pb2.InferenceRequest(
            service_id=service_id,
            feature_vector=data[i].tolist(),
            window_id=f"w{i}", span_id=f"s{i}", trace_id="e2e-trace",
        )
        result = servicer.Infer(request, context=None)

        model_used_counts[result.model_used] = model_used_counts.get(result.model_used, 0) + 1
        if result.model_used == "TRANSFORMER" and first_transformer_tick is None:
            first_transformer_tick = i

        # WINDOW-level ground truth: was ANY reading in the actual window the
        # model just scored anomalous — not just this single tick. Point-level
        # labeling was the bug in the first E2E run.
        window_start = max(0, i - seq_len + 1)
        window_is_anomaly = bool(mask[window_start:i + 1].any())

        if result.is_anomaly:
            confirmed_alerts.append((i, window_is_anomaly, result.reconstruction_error, result.model_used))

        if window_is_anomaly:
            tp += result.is_anomaly
            fn += not result.is_anomaly
        else:
            fp += result.is_anomaly
            tn += not result.is_anomaly

    print(f"[E2E] Model usage across {n_steps} ticks: {model_used_counts}")
    print(f"[E2E] Transformer first kicked in at tick {first_transformer_tick} "
          f"(expected: tick {seq_len - 1})")
    print(f"[E2E] Confirmed (debounced) alerts: {len(confirmed_alerts)} / {n_steps} "
          f"({100 * len(confirmed_alerts) / n_steps:.1f}%)")
    print(f"[E2E] Against WINDOW-level ground truth: TP={tp} FP={fp} FN={fn} TN={tn}")
    if tp + fp > 0:
        print(f"[E2E] Precision={tp / (tp + fp):.4f}")
    if tp + fn > 0:
        print(f"[E2E] Recall={tp / (tp + fn):.4f}")

    print("[E2E] Sample alerts:")
    for tick, was_real_anomaly, err, model_used in confirmed_alerts[:10]:
        print(f"    tick={tick} model={model_used} window_is_anomaly={was_real_anomaly} error={err:.4f}")
    if len(confirmed_alerts) > 10:
        print(f"    ... and {len(confirmed_alerts) - 10} more")


if __name__ == "__main__":
    run_e2e_test(n_steps=500, anomaly_duty_cycle=0.05)