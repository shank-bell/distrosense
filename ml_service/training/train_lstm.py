import torch
import torch.nn as nn
import numpy as np
from torch.utils.data import DataLoader, random_split
from ml_service.models.lstm_autoencoder import LSTMAutoencoder
from ml_service.models.threshold import compute_threshold, save_threshold
from ml_service.training.dataset import MetricWindowDataset, generate_synthetic_data
from ml_service.config import LSTM_MODEL_PATH
from pathlib import Path


def train(epochs: int = 60, batch_size: int = 32, lr: float = 1e-3):
    print("[TrainLSTM] Generating synthetic training data...")
    raw_data = generate_synthetic_data(n_services=20, n_steps=5000)

    # Normalise
    mean = raw_data.mean(axis=0)
    std  = raw_data.std(axis=0) + 1e-9
    raw_data = (raw_data - mean) / std

    dataset = MetricWindowDataset(raw_data)
    val_size   = int(len(dataset) * 0.2)
    train_size = len(dataset) - val_size
    train_ds, val_ds = random_split(dataset, [train_size, val_size])

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size, shuffle=False)

    model     = LSTMAutoencoder()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    best_val_loss = float("inf")

    print(f"[TrainLSTM] Training for {epochs} epochs...")
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

        # Validation
        model.eval()
        val_errors = []
        with torch.no_grad():
            for batch in val_loader:
                output = model(batch)
                loss   = criterion(output, batch)
                val_errors.append(loss.item())

        val_loss = np.mean(val_errors)
        print(f"Epoch {epoch+1}/{epochs} — train_loss={train_loss/len(train_loader):.4f} val_loss={val_loss:.4f}")

        # Early stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            Path(LSTM_MODEL_PATH).parent.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), LSTM_MODEL_PATH)

    # Compute threshold from validation errors
    stats = compute_threshold(val_errors)
    save_threshold(stats)
    print(f"[TrainLSTM] Done. Best val_loss={best_val_loss:.4f}")
    print(f"[TrainLSTM] Threshold: {stats['threshold']:.4f}")


if __name__ == "__main__":
    train(epochs=60)