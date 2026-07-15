import math
import torch
import torch.nn as nn
from ml_service.config import (
    TRANSFORMER_SEQUENCE_LEN, TRANSFORMER_NUM_FEATURES,
    TRANSFORMER_D_MODEL, TRANSFORMER_N_HEADS, TRANSFORMER_N_LAYERS,
    TRANSFORMER_DIM_FEEDFORWARD, TRANSFORMER_DROPOUT,
    TRANSFORMER_DECODER_HIDDEN1, TRANSFORMER_DECODER_HIDDEN2,
)


class PositionalEncoding(nn.Module):
    def __init__(self, d_model: int, max_len: int = TRANSFORMER_SEQUENCE_LEN):
        super().__init__()
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float32).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model)
        )
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        self.register_buffer("pe", pe.unsqueeze(0))

    def forward(self, x):
        return x + self.pe[:, :x.size(1), :]


class TransformerEncoderBlock(nn.Module):
    def __init__(self):
        super().__init__()
        self.input_proj = nn.Linear(TRANSFORMER_NUM_FEATURES, TRANSFORMER_D_MODEL)
        self.pos_encoding = PositionalEncoding(TRANSFORMER_D_MODEL, max_len=TRANSFORMER_SEQUENCE_LEN)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=TRANSFORMER_D_MODEL,
            nhead=TRANSFORMER_N_HEADS,
            dim_feedforward=TRANSFORMER_DIM_FEEDFORWARD,
            dropout=TRANSFORMER_DROPOUT,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=TRANSFORMER_N_LAYERS)

    def forward(self, x):
        x = self.input_proj(x)
        x = self.pos_encoding(x)
        x = self.encoder(x)
        return x.mean(dim=1)


class MLPDecoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(TRANSFORMER_D_MODEL, TRANSFORMER_DECODER_HIDDEN1),
            nn.ReLU(),
            nn.Dropout(TRANSFORMER_DROPOUT),
            nn.Linear(TRANSFORMER_DECODER_HIDDEN1, TRANSFORMER_DECODER_HIDDEN2),
            nn.ReLU(),
            nn.Dropout(TRANSFORMER_DROPOUT),
            nn.Linear(TRANSFORMER_DECODER_HIDDEN2, TRANSFORMER_SEQUENCE_LEN * TRANSFORMER_NUM_FEATURES),
        )

    def forward(self, latent):
        out = self.net(latent)
        return out.view(-1, TRANSFORMER_SEQUENCE_LEN, TRANSFORMER_NUM_FEATURES)


class TransformerAutoencoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.encoder = TransformerEncoderBlock()
        self.decoder = MLPDecoder()

    def forward(self, x):
        latent = self.encoder(x)
        return self.decoder(latent)

    def reconstruction_error(self, x: torch.Tensor) -> float:
        self.eval()
        with torch.no_grad():
            reconstructed = self.forward(x)
            loss = nn.MSELoss()(reconstructed, x)
        return float(loss.item())