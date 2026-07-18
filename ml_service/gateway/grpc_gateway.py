import time
import json
from collections import deque
import grpc
import numpy as np
import torch
from concurrent import futures

from ml_service.proto import ml_service_pb2, ml_service_pb2_grpc
from ml_service.models.transformer_autoencoder import TransformerAutoencoder
from ml_service.models.isolation_forest import IsolationForestModel
from ml_service.models.anomaly_debouncer import AnomalyDebouncer
from ml_service.config import (
    GRPC_HOST, GRPC_PORT,
    TRANSFORMER_MODEL_PATH, TRANSFORMER_THRESHOLD_PATH, TRANSFORMER_NORM_STATS_PATH,
    TRANSFORMER_SEQUENCE_LEN, TRANSFORMER_NUM_FEATURES,
    DEBOUNCE_PERSISTENCE_WINDOWS,
)


class MLInferenceServicer(ml_service_pb2_grpc.MLInferenceServiceServicer):
    """
    Transformer autoencoder is the primary model (replaces LSTM entirely).
    Isolation Forest is the warmup-period fallback — it builds its 7-feature
    statistical summary from whatever's in the per-service rolling buffer so
    far (even just 1-2 readings), so warmup ticks get real coverage instead
    of silently defaulting to "not anomalous".
    """
    def __init__(self):
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._device = device

        self._transformer = TransformerAutoencoder().to(device)
        try:
            self._transformer.load_state_dict(
                torch.load(TRANSFORMER_MODEL_PATH, map_location=device)
            )
            self._transformer.eval()
            print("[Gateway] Transformer model loaded")
        except FileNotFoundError:
            print("[Gateway] Transformer model not found — run training first")
            self._transformer = None

        try:
            with open(TRANSFORMER_THRESHOLD_PATH) as f:
                self._threshold = json.load(f)["threshold"]
        except FileNotFoundError:
            print("[Gateway] Transformer threshold not found — using fallback 1.0")
            self._threshold = 1.0

        try:
            with open(TRANSFORMER_NORM_STATS_PATH) as f:
                stats = json.load(f)
            self._norm_mean = np.array(stats["mean"])
            self._norm_std  = np.array(stats["std"])
        except FileNotFoundError:
            print("[Gateway] Norm stats not found — Transformer inference disabled")
            self._transformer = None

        self._iforest = IsolationForestModel()
        try:
            self._iforest.load()
        except FileNotFoundError:
            print("[Gateway] Isolation Forest model not found — run training first")
            self._iforest = None

        self._buffers: dict[str, deque] = {}
        self._debouncers: dict[str, AnomalyDebouncer] = {}

    def _get_buffer(self, service_id: str) -> deque:
        if service_id not in self._buffers:
            self._buffers[service_id] = deque(maxlen=TRANSFORMER_SEQUENCE_LEN)
        return self._buffers[service_id]

    def _get_debouncer(self, service_id: str) -> AnomalyDebouncer:
        if service_id not in self._debouncers:
            self._debouncers[service_id] = AnomalyDebouncer(
                threshold=self._threshold,
                persistence_windows=DEBOUNCE_PERSISTENCE_WINDOWS,
            )
        return self._debouncers[service_id]

    @staticmethod
    def _buffer_to_metrics_window(buffer: deque) -> list[dict]:
        return [
            {
                "cpu_percent":  row[0],
                "latency_p99":  row[1],
                "error_rate":   row[2],
                "request_rate": row[3],
            }
            for row in buffer
        ]

    def Infer(self, request, context):
        t_start = time.time()

        features   = list(request.feature_vector)
        service_id = request.service_id
        anomaly_score = 0.0
        model_used    = "NONE"
        recon_error   = 0.0
        is_anomaly    = False

        buffer = self._get_buffer(service_id)
        if len(features) >= TRANSFORMER_NUM_FEATURES:
            buffer.append(features[:TRANSFORMER_NUM_FEATURES])

        # Primary: Transformer, only once a full real window has built up
        if self._transformer is not None and len(buffer) == TRANSFORMER_SEQUENCE_LEN:
            try:
                window = np.array(buffer, dtype=np.float32)
                window = (window - self._norm_mean) / self._norm_std
                window_t = torch.tensor(window, dtype=torch.float32).unsqueeze(0).to(self._device)

                with torch.no_grad():
                    recon_error = self._transformer.reconstruction_error(window_t)

                anomaly_score = min(1.0, recon_error / self._threshold)
                model_used    = "TRANSFORMER"

                debouncer  = self._get_debouncer(service_id)
                is_anomaly = debouncer.check(recon_error)
            except Exception as e:
                print(f"[Gateway] Transformer inference error: {e}")

        # Fallback: Isolation Forest — covers warmup ticks (buffer not yet
        # full) using whatever real readings have accumulated so far, and
        # covers any Transformer error above.
        if model_used == "NONE" and self._iforest is not None and len(buffer) > 0:
            try:
                metrics_window = self._buffer_to_metrics_window(buffer)
                feat = IsolationForestModel.extract_features(metrics_window)
                anomaly_score = self._iforest.predict(feat)
                model_used    = "ISOLATION_FOREST"
                is_anomaly    = anomaly_score > 0.7
            except Exception as e:
                print(f"[Gateway] IForest error: {e}")

        latency_ms = (time.time() - t_start) * 1000

        anomaly_type = ""
        if is_anomaly and len(features) >= 4:
            max_idx = int(np.argmax(np.abs(features[:4])))
            anomaly_type = [
                "CPU_SPIKE", "LATENCY_BURST",
                "ERROR_RATE_SPIKE", "REQUEST_DROP"
            ][max_idx]

        return ml_service_pb2.InferenceResult(
            service_id           = service_id,
            anomaly_score        = float(anomaly_score),
            model_used           = model_used,
            is_anomaly           = is_anomaly,
            latency_ms           = float(latency_ms),
            anomaly_type         = anomaly_type,
            reconstruction_error = float(recon_error),
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ml_service_pb2_grpc.add_MLInferenceServiceServicer_to_server(
        MLInferenceServicer(), server
    )
    server.add_insecure_port(f"{GRPC_HOST}:{GRPC_PORT}")
    server.start()
    print(f"[MLGateway] gRPC server listening on {GRPC_HOST}:{GRPC_PORT}")
    server.wait_for_termination()