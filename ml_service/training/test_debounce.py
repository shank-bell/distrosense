import json
import numpy as np
import torch
import torch.nn as nn
from sklearn.metrics import precision_recall_fscore_support, confusion_matrix

from ml_service.models.transformer_autoencoder import TransformerAutoencoder
from ml_service.models.anomaly_debouncer import AnomalyDebouncer
from ml_service.training.dataset import generate_synthetic_dataset
from ml_service.config import (
    TRANSFORMER_MODEL_PATH, TRANSFORMER_THRESHOLD_PATH,
    TRANSFORMER_NORM_STATS_PATH, TRANSFORMER_SEQUENCE_LEN
)


def compute_eval_errors(seed: int = 42):
    """
    Runs the model once over a FIXED, seeded eval set and returns the
    raw (error, is_anomaly) trace per service, in time order. Computing
    this once and reusing it for every persistence_windows value is what
    makes the sweep fair — otherwise each setting would be tested against
    different random data, and differences in the results could just be
    data noise, not the debounce setting itself.
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = TransformerAutoencoder().to(device)
    model.load_state_dict(torch.load(TRANSFORMER_MODEL_PATH, map_location=device))
    model.eval()

    with open(TRANSFORMER_THRESHOLD_PATH) as f:
        threshold = json.load(f)["threshold"]
    with open(TRANSFORMER_NORM_STATS_PATH) as f:
        stats = json.load(f)
    mean = np.array(stats["mean"])
    std  = np.array(stats["std"])

    np.random.seed(seed)
    services, masks = generate_synthetic_dataset(
        n_services=10, n_steps=2000, inject_anomalies=True, anomaly_duty_cycle=0.05
    )

    criterion = nn.MSELoss()
    seq_len = TRANSFORMER_SEQUENCE_LEN

    traces = []
    for data, mask in zip(services, masks):
        data = (data - mean) / std
        trace = []
        for i in range(len(data) - seq_len):
            window = data[i:i + seq_len]
            is_anomaly = bool(mask[i:i + seq_len].any())
            window_t = torch.tensor(window, dtype=torch.float32).unsqueeze(0).to(device)
            with torch.no_grad():
                error = criterion(model(window_t), window_t).item()
            trace.append((error, is_anomaly))
        traces.append(trace)

    return traces, threshold


def evaluate(traces, threshold, persistence_windows: int):
    y_true, y_pred_raw, y_pred_deb = [], [], []
    for trace in traces:
        debouncer = AnomalyDebouncer(threshold=threshold, persistence_windows=persistence_windows)
        for error, is_anomaly in trace:
            y_true.append(int(is_anomaly))
            y_pred_raw.append(int(error > threshold))
            y_pred_deb.append(int(debouncer.check(error)))

    def summarize(y_pred):
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_true, y_pred, average="binary", zero_division=0
        )
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        return tp, fp, fn, tn, precision, recall, f1

    return summarize(y_pred_raw), summarize(y_pred_deb)


def sweep(persistence_values=(2, 3, 4, 5)):
    print("[Sweep] Running model once over a fixed eval set...")
    traces, threshold = compute_eval_errors()
    print(f"[Sweep] threshold={threshold:.4f}")

    raw_result = None
    for pw in persistence_values:
        raw_result, deb_result = evaluate(traces, threshold, pw)
        tp, fp, fn, tn, precision, recall, f1 = deb_result
        print(f"[persistence_windows={pw}] TP={tp} FP={fp} FN={fn} TN={tn} | "
              f"precision={precision:.4f} recall={recall:.4f} f1={f1:.4f}")

    tp, fp, fn, tn, precision, recall, f1 = raw_result
    print(f"[RAW, no debounce] TP={tp} FP={fp} FN={fn} TN={tn} | "
          f"precision={precision:.4f} recall={recall:.4f} f1={f1:.4f}")


if __name__ == "__main__":
    sweep(persistence_values=(2, 3, 4, 5))