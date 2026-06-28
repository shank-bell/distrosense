import torch
import numpy as np
from ts.torch_handler.base_handler import BaseHandler
from ml_service.models.lstm_autoencoder import LSTMAutoencoder
from ml_service.models.threshold import load_threshold
from ml_service.config import LSTM_SEQUENCE_LEN, LSTM_NUM_FEATURES


class LSTMHandler(BaseHandler):
    """
    TorchServe custom handler for LSTM autoencoder.
    LLD: POST /predictions/lstm
    Input: tensor shape (1, 60, 4)
    Output: reconstruction_error float
    """
    def initialize(self, context):
        self.model     = LSTMAutoencoder()
        self.threshold = load_threshold()
        self.model.eval()

    def preprocess(self, data):
        features = data[0].get("data") or data[0].get("body")
        if isinstance(features, list) and len(features) >= LSTM_NUM_FEATURES:
            feat_vec = features[:LSTM_NUM_FEATURES]
            sequence = np.array(
                [feat_vec] * LSTM_SEQUENCE_LEN,
                dtype=np.float32
            )
            return torch.tensor(sequence).unsqueeze(0)
        raise ValueError(f"Expected {LSTM_NUM_FEATURES} features, got {features}")

    def inference(self, inputs):
        with torch.no_grad():
            reconstructed = self.model(inputs)
        loss = torch.nn.MSELoss()(reconstructed, inputs)
        return float(loss.item())

    def postprocess(self, inference_output):
        threshold = self.threshold.get("threshold", 0.11)
        return [{
            "reconstruction_error": inference_output,
            "threshold":            threshold,
            "anomaly_score":        min(1.0, inference_output / threshold),
        }]