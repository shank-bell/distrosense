import time
import grpc
import requests
import numpy as np
import torch
from concurrent import futures
from ml_service.proto import ml_service_pb2, ml_service_pb2_grpc
from ml_service.models.lstm_autoencoder import LSTMAutoencoder
from ml_service.models.isolation_forest import IsolationForestModel
from ml_service.models.threshold import load_threshold
from ml_service.config import (
    GRPC_HOST, GRPC_PORT,
    TORCHSERVE_URL, TORCHSERVE_TIMEOUT,
    LSTM_SEQUENCE_LEN, LSTM_NUM_FEATURES,
    ANOMALY_SCORE_THRESHOLD,
)


class MLInferenceServicer(ml_service_pb2_grpc.MLInferenceServiceServicer):
    """
    LLD: gRPC gateway — routes to TorchServe (LSTM) or Isolation Forest fallback
    if latency > 50ms → Isolation Forest
    """
    def __init__(self):
        self._iforest   = IsolationForestModel()
        self._threshold = load_threshold()
        self._lstm      = LSTMAutoencoder()

        # Try loading trained models
        try:
            self._iforest.load()
        except FileNotFoundError:
            print("[Gateway] Isolation Forest model not found — run training first")
            self._iforest = None

        try:
            import os
            from ml_service.config import LSTM_MODEL_PATH
            if os.path.exists(LSTM_MODEL_PATH):
                self._lstm.load_state_dict(torch.load(LSTM_MODEL_PATH, map_location="cpu"))
                self._lstm.eval()
                print("[Gateway] LSTM model loaded")
            else:
                print("[Gateway] LSTM model not found — run training first")
                self._lstm = None
        except Exception as e:
            print(f"[Gateway] LSTM load error: {e}")
            self._lstm = None

    def Infer(self, request, context):
        t_start = time.time()

        features     = list(request.feature_vector)
        service_id   = request.service_id
        anomaly_score = 0.0
        model_used    = "NONE"
        recon_error   = 0.0

        # Step 1: Try TorchServe (LSTM) — LLD: POST /predictions/lstm
        torchserve_success = False
        try:
            payload  = {"data": features}
            response = requests.post(
                TORCHSERVE_URL,
                json=payload,
                timeout=TORCHSERVE_TIMEOUT,
            )
            if response.status_code == 200:
                result        = response.json()
                recon_error   = float(result.get("reconstruction_error", 0.0))
                threshold     = self._threshold.get("threshold", 0.11)
                anomaly_score = min(1.0, recon_error / threshold)
                model_used    = "LSTM"
                torchserve_success = True
        except Exception:
            pass

        # Step 2: Fallback to local LSTM if TorchServe unavailable
        if not torchserve_success and self._lstm is not None:
            try:
                if len(features) >= LSTM_NUM_FEATURES:
                    # Repeat last feature vector to fill sequence length
                    feat_vec = features[:LSTM_NUM_FEATURES]
                    sequence = np.array([feat_vec] * LSTM_SEQUENCE_LEN, dtype=np.float32)
                    tensor   = torch.tensor(sequence).unsqueeze(0)
                    recon_error   = self._lstm.reconstruction_error(tensor)
                    threshold     = self._threshold.get("threshold", 0.11)
                    anomaly_score = min(1.0, recon_error / threshold)
                    model_used    = "LSTM_LOCAL"
            except Exception as e:
                print(f"[Gateway] Local LSTM error: {e}")

        # Step 3: Isolation Forest fallback
        if model_used == "NONE" and self._iforest is not None:
            try:
                anomaly_score = self._iforest.predict(features)
                model_used    = "ISOLATION_FOREST"
            except Exception as e:
                print(f"[Gateway] IForest error: {e}")

        latency_ms = (time.time() - t_start) * 1000

        # LLD: is_anomaly if score > 0.7
        is_anomaly = anomaly_score > ANOMALY_SCORE_THRESHOLD

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