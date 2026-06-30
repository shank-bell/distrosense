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
    LLD: 2-layer LSTM decoder reconstructs original 60-step sequence.
    Fixed: feeds the LSTM's own previous output back as next input (autoregressive),
    so each timestep gets distinct input instead of a repeated constant vector.
    """
    def __init__(self):
        super().__init__()
        self.latent_to_hidden = nn.Linear(LSTM_LATENT_SIZE, LSTM_HIDDEN_SIZE)
        self.lstm = nn.LSTM(
            input_size  = LSTM_NUM_FEATURES,
            hidden_size = LSTM_HIDDEN_SIZE,
            num_layers  = 2,
            batch_first = True,
            dropout     = LSTM_DROPOUT,
        )
        self.output_layer = nn.Linear(LSTM_HIDDEN_SIZE, LSTM_NUM_FEATURES)

    def forward(self, latent, seq_len=LSTM_SEQUENCE_LEN):
        batch_size = latent.size(0)

        # Initialise decoder hidden state from the latent vector
        h0 = self.latent_to_hidden(latent).unsqueeze(0).repeat(2, 1, 1)  # (num_layers, batch, hidden)
        c0 = torch.zeros_like(h0)

        # Start token: zeros (no "previous" output yet)
        decoder_input = torch.zeros(batch_size, 1, LSTM_NUM_FEATURES, device=latent.device)

        outputs = []
        hidden = (h0, c0)
        for _ in range(seq_len):
            out, hidden = self.lstm(decoder_input, hidden)
            step_output = self.output_layer(out)  # (batch, 1, num_features)
            outputs.append(step_output)
            decoder_input = step_output  # feed prediction back in as next input

        return torch.cat(outputs, dim=1)  # (batch, seq_len, num_features)


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