import numpy as np
import torch
from torch.utils.data import Dataset
from ml_service.config import LSTM_SEQUENCE_LEN, LSTM_NUM_FEATURES


def generate_service_series(n_steps: int, inject_anomalies: bool = False,
                             anomaly_duty_cycle: float = 0.05, burst_len: int = 15):
    """
    anomaly_duty_cycle: the ACTUAL target fraction of time spent inside an
    anomaly (e.g. 0.05 = 5%). This replaces the old anomaly_prob, which was
    a per-step trigger chance — that number alone doesn't control the real
    duty cycle once burst_len is factored in; this does the math to solve
    for the correct per-step probability instead of guessing.
    """
    cpu        = np.random.normal(40, 5, n_steps)
    latency    = np.random.normal(120, 15, n_steps)
    error_rate = np.random.normal(0.01, 0.002, n_steps)
    req_rate   = np.random.normal(200, 20, n_steps)

    anomaly_mask = np.zeros(n_steps, dtype=bool)

    if inject_anomalies:
        # duty = burst_len / (1/p + burst_len)  =>  p = duty / (burst_len * (1 - duty))
        q = anomaly_duty_cycle
        p = q / (burst_len * (1 - q))

        t = 0
        while t < n_steps - burst_len:
            if np.random.random() < p:
                kind = np.random.choice(["cpu_spike", "latency_burst", "error_spike", "request_drop"])
                end = t + burst_len
                if kind == "cpu_spike":
                    cpu[t:end] += np.random.uniform(30, 50)
                elif kind == "latency_burst":
                    latency[t:end] += np.random.uniform(100, 300)
                elif kind == "error_spike":
                    error_rate[t:end] += np.random.uniform(0.1, 0.3)
                elif kind == "request_drop":
                    req_rate[t:end] *= np.random.uniform(0.1, 0.3)
                anomaly_mask[t:end] = True
                t = end
            else:
                t += 1

    cpu        = np.clip(cpu, 0, 100)
    latency    = np.clip(latency, 0, None)
    error_rate = np.clip(error_rate, 0, 1)
    req_rate   = np.clip(req_rate, 0, None)

    service_data = np.stack([cpu, latency, error_rate, req_rate], axis=1)
    return service_data, anomaly_mask


def generate_synthetic_dataset(n_services: int = 100, n_steps: int = 5000,
                                inject_anomalies: bool = False, anomaly_duty_cycle: float = 0.05):
    services, masks = [], []
    for _ in range(n_services):
        data, mask = generate_service_series(
            n_steps, inject_anomalies=inject_anomalies, anomaly_duty_cycle=anomaly_duty_cycle
        )
        services.append(data)
        masks.append(mask)
    return services, masks

def chronological_split(service_list, val_fraction: float = 0.2):
    """
    Splits each service's OWN timeline at the (1 - val_fraction) mark —
    first part -> train, last part -> val. Done per-service, before
    windowing, so no window ever crosses the train/val boundary, and
    (combined with per-service windowing below) none crosses a service
    boundary either. Replaces the old random_split, which shuffled
    overlapping windows and leaked near-duplicates across the split.
    """
    train_services, val_services = [], []
    for data in service_list:
        split_idx = int(len(data) * (1 - val_fraction))
        train_services.append(data[:split_idx])
        val_services.append(data[split_idx:])
    return train_services, val_services


class MetricWindowDataset(Dataset):
    """
    Builds sliding windows PER SERVICE from a list of per-service arrays
    — a window is never built across two different services.
    Pass mask_list to also get a per-window anomaly label (for the
    labeled eval set); omit it for plain training data.
    """
    def __init__(self, service_list, seq_len: int = LSTM_SEQUENCE_LEN, mask_list=None):
        self.seq_len = seq_len
        windows, labels = [], []
        for idx, data in enumerate(service_list):
            mask = mask_list[idx] if mask_list is not None else None
            for i in range(len(data) - seq_len):
                windows.append(data[i:i + seq_len])
                if mask is not None:
                    labels.append(bool(mask[i:i + seq_len].any()))
        self.windows = np.array(windows, dtype=np.float32)
        self.labels = np.array(labels, dtype=bool) if mask_list is not None else None

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, idx):
        window = torch.tensor(self.windows[idx], dtype=torch.float32)
        if self.labels is not None:
            return window, bool(self.labels[idx])
        return window