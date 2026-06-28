import joblib
import numpy as np
from pathlib import Path
from sklearn.ensemble import IsolationForest
from ml_service.config import (
    IFOREST_N_ESTIMATORS,
    IFOREST_CONTAMINATION,
    IFOREST_MODEL_PATH,
)


class IsolationForestModel:
    """
    LLD: n_estimators=100, contamination=0.05
    Features: [mean, std, p95, p99, max, error_rate, req_rate]
    Score normalised to [0, 1]
    """
    def __init__(self):
        self._model = None

    def train(self, X: np.ndarray):
        self._model = IsolationForest(
            n_estimators  = IFOREST_N_ESTIMATORS,
            contamination = IFOREST_CONTAMINATION,
            random_state  = 42,
            n_jobs        = -1,
        )
        self._model.fit(X)
        print(f"[IsolationForest] Trained on {X.shape[0]} samples")

    def save(self):
        Path(IFOREST_MODEL_PATH).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._model, IFOREST_MODEL_PATH)
        print(f"[IsolationForest] Saved to {IFOREST_MODEL_PATH}")

    def load(self):
        if not Path(IFOREST_MODEL_PATH).exists():
            raise FileNotFoundError(
                f"Isolation Forest model not found at {IFOREST_MODEL_PATH}. "
                f"Run training/train_iforest.py first."
            )
        self._model = joblib.load(IFOREST_MODEL_PATH)
        print("[IsolationForest] Loaded from disk")

    def predict(self, features: list[float]) -> float:
        """
        LLD: score = -model.decision_function(features), normalised to [0,1]
        """
        if self._model is None:
            raise RuntimeError("Model not loaded. Call load() first.")
        X = np.array(features).reshape(1, -1)
        raw_score = -self._model.decision_function(X)[0]
        # Normalise to [0, 1] using sigmoid-like scaling
        normalised = float(1 / (1 + np.exp(-raw_score * 5)))
        return normalised

    @staticmethod
    def extract_features(metrics_window: list[dict]) -> list[float]:
        """
        LLD: features = [mean, std, p95, p99, max, error_rate, req_rate]
        """
        if not metrics_window:
            return [0.0] * 7

        cpu_vals  = [m.get("cpu_percent", 0.0)  for m in metrics_window]
        lat_vals  = [m.get("latency_p99", 0.0)  for m in metrics_window]
        err_vals  = [m.get("error_rate", 0.0)   for m in metrics_window]
        req_vals  = [m.get("request_rate", 0.0) for m in metrics_window]

        all_vals = cpu_vals + lat_vals
        return [
            float(np.mean(all_vals)),
            float(np.std(all_vals)),
            float(np.percentile(all_vals, 95)),
            float(np.percentile(all_vals, 99)),
            float(np.max(all_vals)),
            float(np.mean(err_vals)),
            float(np.mean(req_vals)),
        ]