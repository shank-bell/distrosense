import numpy as np
from ml_service.models.isolation_forest import IsolationForestModel
from ml_service.training.dataset import generate_synthetic_dataset

WINDOW_SIZE = 60


def train():
    print("[TrainIForest] Generating synthetic training data (100 services, normal only)...")
    services, _ = generate_synthetic_dataset(n_services=100, n_steps=5000, inject_anomalies=False)

    features = []
    for service_data in services:
        # Non-overlapping windows, built PER SERVICE — same fix we made for
        # the Transformer's dataset.py: never let a window span two services.
        for i in range(0, len(service_data) - WINDOW_SIZE, WINDOW_SIZE):
            window = service_data[i:i + WINDOW_SIZE]
            metrics_window = [
                {
                    "cpu_percent":  row[0],
                    "latency_p99":  row[1],
                    "error_rate":   row[2],
                    "request_rate": row[3],
                }
                for row in window
            ]
            feat = IsolationForestModel.extract_features(metrics_window)
            features.append(feat)

    X = np.array(features)
    print(f"[TrainIForest] Training on {X.shape[0]} windows...")

    model = IsolationForestModel()
    model.train(X)
    model.save()
    print("[TrainIForest] Done.")


if __name__ == "__main__":
    train()