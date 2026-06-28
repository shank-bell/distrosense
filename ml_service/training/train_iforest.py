import numpy as np
from ml_service.models.isolation_forest import IsolationForestModel
from ml_service.training.dataset import generate_synthetic_data


def train():
    print("[TrainIForest] Generating synthetic training data...")
    raw_data = generate_synthetic_data(n_services=20, n_steps=5000)

    # Build feature vectors: [mean, std, p95, p99, max, error_rate, req_rate]
    window_size = 60
    features = []
    for i in range(0, len(raw_data) - window_size, window_size):
        window = raw_data[i:i + window_size]
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