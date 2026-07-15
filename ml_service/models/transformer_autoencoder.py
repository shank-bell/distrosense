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
    TRANSFORMER_MODEL_PATH, TRANSFORMER_THRESHOLD_PATH, THRESHOLD_SIGMA_MULTIPLIER
)


def normalize_services(service_list, mean, std):
    return [(data - mean) / std for data in service_list]


def train(epochs: int = 60, batch_size: int = 128, lr: float = 1e-3):
    print("[TrainTransformer] Generating synthetic training data (100 services)...")
    services, _ = generate_synthetic_dataset(n_services=100, n_steps=5000, inject_anomalies=False)

    train_services, val_services = chronological_split(services, val_fraction=0.2)

    # Normalize using TRAIN stats only — computing mean/std from val too
    # would leak validation information into the normalization itself.
    train_concat = np.concatenate(train_services, axis=0)
    mean = train_concat.mean(axis=0)
    std  = train_concat.std(axis=0) + 1e-9

    train_services = normalize_services(train_services, mean, std)
    val_services   = normalize_services(val_services, mean, std)

    train_ds = MetricWindowDataset(train_services)
    val_ds   = MetricWindowDataset(val_services)

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size, shuffle=False)

    model     = TransformerAutoencoder()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    best_val_loss = float("inf")

    print(f"[TrainTransformer] {len(train_ds)} train windows, {len(val_ds)} val windows")
    print(f"[TrainTransformer] Training for {epochs} epochs...")

    for epoch in range(epochs):
        model.train()
        train_loss = 0.0
        for batch in train_loader:
            optimizer.zero_grad()
            output = model(batch)
            loss   = criterion(output, batch)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()

        model.eval()
        val_errors = []
        with torch.no_grad():
            for batch in val_loader:
                output = model(batch)
                loss   = criterion(output, batch)
                val_errors.append(loss.item())

        val_loss = np.mean(val_errors)
        print(f"Epoch {epoch+1}/{epochs} — train_loss={train_loss/len(train_loader):.4f} val_loss={val_loss:.4f}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            Path(TRANSFORMER_MODEL_PATH).parent.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), TRANSFORMER_MODEL_PATH)

    # Threshold from validation reconstruction error (normal data only)
    mean_err = float(np.mean(val_errors))
    std_err  = float(np.std(val_errors))
    threshold = mean_err + THRESHOLD_SIGMA_MULTIPLIER * std_err
    stats = {"mean": mean_err, "std": std_err, "threshold": threshold, "sigma": THRESHOLD_SIGMA_MULTIPLIER}

    Path(TRANSFORMER_THRESHOLD_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(TRANSFORMER_THRESHOLD_PATH, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"[TrainTransformer] Done. Best val_loss={best_val_loss:.4f}")
    print(f"[TrainTransformer] Threshold: {threshold:.4f}")

    # --- Honest evaluation: does reconstruction error actually separate anomalies? ---
    # This is the check that never existed before — training/validating purely on
    # normal data tells you nothing about whether the threshold actually catches anomalies.
    print("[TrainTransformer] Building small labeled eval set to check anomaly separation...")
    eval_services, eval_masks = generate_synthetic_dataset(n_services=10, n_steps=2000, inject_anomalies=True)
    eval_services = normalize_services(eval_services, mean, std)
    eval_ds = MetricWindowDataset(eval_services, mask_list=eval_masks)

    model.eval()
    normal_errors, anomaly_errors = [], []
    with torch.no_grad():
        for window, is_anomaly in eval_ds:
            window = window.unsqueeze(0)
            output = model(window)
            err = criterion(output, window).item()
            (anomaly_errors if is_anomaly else normal_errors).append(err)

    print(f"[Eval] Normal windows: {len(normal_errors)}, mean error={np.mean(normal_errors):.4f}")
    print(f"[Eval] Anomalous windows: {len(anomaly_errors)}, mean error={np.mean(anomaly_errors):.4f}")

    caught = sum(1 for e in anomaly_errors if e > threshold)
    false_alarms = sum(1 for e in normal_errors if e > threshold)
    print(f"[Eval] Anomalies above threshold: {caught}/{len(anomaly_errors)}")
    print(f"[Eval] Normal windows flagged as false alarms: {false_alarms}/{len(normal_errors)}")


if __name__ == "__main__":
    train(epochs=60)