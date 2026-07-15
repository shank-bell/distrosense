import json
import numpy as np
import torch
import torch.nn as nn

from ml_service.models.transformer_autoencoder import TransformerAutoencoder
from ml_service.models.anomaly_debouncer import AnomalyDebouncer
from ml_service.training.dataset import generate_synthetic_dataset
from ml_service.config import (
    TRANSFORMER_MODEL_PATH, TRANSFORMER_THRESHOLD_PATH,
    TRANSFORMER_NORM_STATS_PATH, TRANSFORMER_SEQUENCE_LEN
)


def test_debounce(persistence_windows: int = 3):
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

    print(f"[TestDebounce] threshold={threshold:.4f}, persistence_windows={persistence_windows}")

    services, masks = generate_synthetic_dataset(
        n_services=10, n_steps=2000, inject_anomalies=True, anomaly_duty_cycle=0.05
    )

    criterion = nn.MSELoss()
    seq_len = TRANSFORMER_SEQUENCE_LEN

    raw_tp = raw_fp = raw_fn = raw_tn = 0
    deb_tp = deb_fp = deb_fn = deb_tn = 0

    for data, mask in zip(services, masks):
        data = (data - mean) / std
        debouncer = AnomalyDebouncer(threshold=threshold, persistence_windows=persistence_windows)

        # Walk windows IN TIME ORDER, one service at a time — debounce
        # only makes sense against a real chronological sequence.
        for i in range(len(data) - seq_len):
            window = data[i:i + seq_len]
            is_anomaly = bool(mask[i:i + seq_len].any())

            window_t = torch.tensor(window, dtype=torch.float32).unsqueeze(0).to(device)
            with torch.no_grad():
                error = criterion(model(window_t), window_t).item()

            raw_alert = error > threshold
            confirmed = debouncer.check(error)

            if is_anomaly:
                raw_tp += raw_alert; raw_fn += not raw_alert
                deb_tp += confirmed; deb_fn += not confirmed
            else:
                raw_fp += raw_alert; raw_tn += not raw_alert
                deb_fp += confirmed; deb_tn += not confirmed

    def summarize(label, tp, fp, fn, tn):
        recall    = tp / (tp + fn) if (tp + fn) else float("nan")
        precision = tp / (tp + fp) if (tp + fp) else float("nan")
        fpr       = fp / (fp + tn) if (fp + tn) else float("nan")
        print(f"[{label}] TP={tp} FP={fp} FN={fn} TN={tn} | "
              f"recall={recall:.4f} precision={precision:.4f} false_alarm_rate={fpr:.4f}")

    summarize("RAW (no debounce)", raw_tp, raw_fp, raw_fn, raw_tn)
    summarize("DEBOUNCED", deb_tp, deb_fp, deb_fn, deb_tn)


if __name__ == "__main__":
    test_debounce(persistence_windows=3)