import torch
import torch.nn as nn
from ml_service.config import (
    LSTM_SEQUENCE_LEN, LSTM_NUM_FEATURES,
    LSTM_HIDDEN_SIZE, LSTM_LATENT_SIZE, LSTM_DROPOUT
)


class LSTMEncoder(nn.Module):
    """
    LLD: 2-layer LSTM encoder, hidden=64, latent=32
    Input shape: (batch, 60, 4)
    """
    def __init__(self):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size   = LSTM_NUM_FEATURES,
            hidden_size  = LSTM_HIDDEN_SIZE,
            num_layers   = 2,
            batch_first  = True,
            dropout      = LSTM_DROPOUT,
        )
        self.fc = nn.Linear(LSTM_HIDDEN_SIZE, LSTM_LATENT_SIZE)

    def forward(self, x):
        # x: (batch, seq_len, features)
        _, (hidden, _) = self.lstm(x)
        # hidden: (num_layers, batch, hidden_size)
        last_hidden = hidden[-1]
        latent = self.fc(last_hidden)
        return latent


class LSTMDecoder(nn.Module):
    """
    LLD: 2-layer LSTM decoder reconstructs original 60-step sequence
    """
    def __init__(self):
        super().__init__()
        self.fc   = nn.Linear(LSTM_LATENT_SIZE, LSTM_HIDDEN_SIZE)
        self.lstm = nn.LSTM(
            input_size  = LSTM_HIDDEN_SIZE,
            hidden_size = LSTM_NUM_FEATURES,
            num_layers  = 2,
            batch_first = True,
            dropout     = LSTM_DROPOUT,
        )

    def forward(self, latent, seq_len=LSTM_SEQUENCE_LEN):
        # Expand latent to sequence
        x = self.fc(latent)
        x = x.unsqueeze(1).repeat(1, seq_len, 1)
        output, _ = self.lstm(x)
        return output


class LSTMAutoencoder(nn.Module):
    """
    LLD: Full autoencoder
    Loss = MSE between input and reconstruction
    Threshold = mean + 3σ over validation set
    """
    def __init__(self):
        super().__init__()
        self.encoder = LSTMEncoder()
        self.decoder = LSTMDecoder()

    def forward(self, x):
        latent       = self.encoder(x)
        reconstructed = self.decoder(latent)
        return reconstructed

    def reconstruction_error(self, x: torch.Tensor) -> float:
        self.eval()
        with torch.no_grad():
            reconstructed = self.forward(x)
            loss = nn.MSELoss()(reconstructed, x)
        return float(loss.item())