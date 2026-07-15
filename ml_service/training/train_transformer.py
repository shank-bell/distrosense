import json
import numpy as np
import torch
import torch.nn as nn
from pathlib import Path
from torch.utils.data import DataLoader

from ml_service.models.transformer_autoencoder import TransformerAutoencoder
from ml_service.training.dataset import (
    generate_synthetic_dataset, chronological_split, MetricWindowDataset
)
from ml_service.config import (
    TRANSFORMER_MODEL_PATH, TRANSFORMER_THRESHOLD_PATH,
    TRANSFORMER_NORM_STATS_PATH, THRESHOLD_SIGMA_MULTIPLIER
)


def normalize_services(service_list, mean, std):
    return [(data - mean) / std for data in service_list]


def train(epochs: int = 60, batch_size: int = 128, lr: float = 1e-3):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[TrainTransformer] Using device: {device}")

    print("[TrainTransformer] Generating synthetic training data (100 services)...")
    services, _ = generate_synthetic_dataset(n_services=100, n_steps=5000, inject_anomalies=False)
    train_services, val_services = chronological_split(services, val_fraction=0.2)

    train_concat = np.concatenate(train_services, axis=0)
    mean = train_concat.mean(axis=0)
    std  = train_concat.std(axis=0) + 1e-9

    # Save normalization stats — needed by anything that evaluates or
    # deploys this model later, since new data must be normalized with
    # these exact same numbers, not recomputed from scratch.
    Path(TRANSFORMER_NORM_STATS_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(TRANSFORMER_NORM_STATS_PATH, "w") as f:
        json.dump({"mean": mean.tolist(), "std": std.tolist()}, f, indent=2)

    train_services = normalize_services(train_services, mean, std)
    val_services   = normalize_services(val_services, mean, std)

    train_ds = MetricWindowDataset(train_services)
    val_ds   = MetricWindowDataset(val_services)

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size, shuffle=False)

    model     = TransformerAutoencoder().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    best_val_loss = float("inf")
    print(f"[TrainTransformer] {len(train_ds)} train windows, {len(val_ds)} val windows")
    print(f"[TrainTransformer] Training for {epochs} epochs...")

    for epoch in range(epochs):
        model.train()
        train_loss = 0.0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            output = model(batch)
            loss = criterion(output, batch)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()

        model.eval()
        val_errors = []
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                output = model(batch)
                val_errors.append(criterion(output, batch).item())

        val_loss = np.mean(val_errors)
        print(f"Epoch {epoch+1}/{epochs} — train_loss={train_loss/len(train_loader):.4f} val_loss={val_loss:.4f}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            Path(TRANSFORMER_MODEL_PATH).parent.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), TRANSFORMER_MODEL_PATH)

    mean_err, std_err = float(np.mean(val_errors)), float(np.std(val_errors))
    threshold = mean_err + THRESHOLD_SIGMA_MULTIPLIER * std_err

    Path(TRANSFORMER_THRESHOLD_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(TRANSFORMER_THRESHOLD_PATH, "w") as f:
        json.dump({"mean": mean_err, "std": std_err, "threshold": threshold}, f, indent=2)

    print(f"[TrainTransformer] Done. Best val_loss={best_val_loss:.4f} | Threshold={threshold:.4f}")

    print("[TrainTransformer] Checking anomaly separation on a labeled eval set...")
    eval_services, eval_masks = generate_synthetic_dataset(
        n_services=10, n_steps=2000, inject_anomalies=True, anomaly_duty_cycle=0.05
    )
    eval_services = normalize_services(eval_services, mean, std)
    eval_ds = MetricWindowDataset(eval_services, mask_list=eval_masks)

    model.eval()
    normal_errors, anomaly_errors = [], []
    with torch.no_grad():
        for window, is_anomaly in eval_ds:
            window = window.unsqueeze(0).to(device)
            err = criterion(model(window), window).item()
            (anomaly_errors if is_anomaly else normal_errors).append(err)

    print(f"[Eval] Normal windows: {len(normal_errors)}, mean error={np.mean(normal_errors):.4f}")
    print(f"[Eval] Anomalous windows: {len(anomaly_errors)}, mean error={np.mean(anomaly_errors):.4f}")
    print(f"[Eval] Anomalies caught above threshold: {sum(e > threshold for e in anomaly_errors)}/{len(anomaly_errors)}")
    print(f"[Eval] False alarms on normal windows: {sum(e > threshold for e in normal_errors)}/{len(normal_errors)}")


if __name__ == "__main__":
    train(epochs=60)