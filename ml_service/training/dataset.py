import numpy as np
import torch
from torch.utils.data import Dataset
from ml_service.config import LSTM_SEQUENCE_LEN, LSTM_NUM_FEATURES


class MetricWindowDataset(Dataset):
    """
    Builds sliding window dataset for LSTM training.
    Each sample is a (seq_len, num_features) tensor.
    LLD: seq_len=60, features=[cpu, latency, error_rate, request_rate]
    """
    def __init__(self, data: np.ndarray, seq_len: int = LSTM_SEQUENCE_LEN):
        self.seq_len = seq_len
        self.windows = []
        for i in range(len(data) - seq_len):
            self.windows.append(data[i:i + seq_len])
        self.windows = np.array(self.windows, dtype=np.float32)

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, idx):
        return torch.tensor(self.windows[idx], dtype=torch.float32)


def generate_synthetic_data(
    n_services: int = 10,
    n_steps: int = 5000,
) -> np.ndarray:
    """
    Generates synthetic normal metric data for LSTM training.
    Used when real historical data isn't available.
    """
    data = []
    for _ in range(n_services):
        cpu        = np.random.normal(40, 5, n_steps)
        latency    = np.random.normal(120, 15, n_steps)
        error_rate = np.random.normal(0.01, 0.002, n_steps)
        req_rate   = np.random.normal(200, 20, n_steps)

        # Clip to realistic ranges
        cpu        = np.clip(cpu, 0, 100)
        latency    = np.clip(latency, 0, None)
        error_rate = np.clip(error_rate, 0, 1)
        req_rate   = np.clip(req_rate, 0, None)

        service_data = np.stack([cpu, latency, error_rate, req_rate], axis=1)
        data.append(service_data)

    return np.concatenate(data, axis=0)