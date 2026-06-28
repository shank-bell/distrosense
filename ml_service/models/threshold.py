import json
import numpy as np
from pathlib import Path
from ml_service.config import THRESHOLD_PATH, THRESHOLD_SIGMA_MULTIPLIER


def compute_threshold(errors: list[float]) -> dict:
    """
    LLD: threshold = mean + 3σ over validation set
    """
    mean = float(np.mean(errors))
    std  = float(np.std(errors))
    threshold = mean + THRESHOLD_SIGMA_MULTIPLIER * std
    return {
        "mean":      mean,
        "std":       std,
        "threshold": threshold,
        "sigma":     THRESHOLD_SIGMA_MULTIPLIER,
    }


def save_threshold(stats: dict):
    Path(THRESHOLD_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(THRESHOLD_PATH, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"[Threshold] Saved: mean={stats['mean']:.4f} "
          f"std={stats['std']:.4f} threshold={stats['threshold']:.4f}")


def load_threshold() -> dict:
    if not Path(THRESHOLD_PATH).exists():
        # Default threshold if not trained yet
        return {"mean": 0.05, "std": 0.02, "threshold": 0.11}
    with open(THRESHOLD_PATH) as f:
        return json.load(f)