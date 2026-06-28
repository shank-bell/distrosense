import os
from dotenv import load_dotenv

load_dotenv()

# gRPC server — LLD: port 50052
GRPC_HOST = os.getenv("ML_GRPC_HOST", "0.0.0.0")
GRPC_PORT = int(os.getenv("ML_GRPC_PORT", "50052"))

# TorchServe — LLD: POST /predictions/lstm
TORCHSERVE_HOST    = os.getenv("TORCHSERVE_HOST", "localhost")
TORCHSERVE_PORT    = int(os.getenv("TORCHSERVE_PORT", "8080"))
TORCHSERVE_URL     = f"http://{TORCHSERVE_HOST}:{TORCHSERVE_PORT}/predictions/lstm"
TORCHSERVE_TIMEOUT = 0.05  # 50ms — LLD: if latency > 50ms → fallback

# LSTM model — LLD: input shape (batch, 60, 4)
LSTM_SEQUENCE_LEN  = 60
LSTM_NUM_FEATURES  = 4     # cpu, latency, error_rate, request_rate
LSTM_HIDDEN_SIZE   = 64
LSTM_LATENT_SIZE   = 32
LSTM_DROPOUT       = 0.2

# Anomaly threshold — LLD: mean + 3σ
THRESHOLD_SIGMA_MULTIPLIER = 3.0

# Isolation Forest — LLD: n_estimators=100, contamination=0.05
IFOREST_N_ESTIMATORS  = 100
IFOREST_CONTAMINATION = 0.05

# LLD: is_anomaly if score > 0.7
ANOMALY_SCORE_THRESHOLD = 0.7

# Model paths
MODEL_DIR         = os.path.join(os.path.dirname(__file__), "torchserve", "model_store")
LSTM_MODEL_PATH   = os.path.join(MODEL_DIR, "lstm_autoencoder.pt")
IFOREST_MODEL_PATH = os.path.join(MODEL_DIR, "iso_forest.pkl")
THRESHOLD_PATH    = os.path.join(MODEL_DIR, "threshold.json")