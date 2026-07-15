from collections import deque


class AnomalyDebouncer:
    """
    Requires reconstruction error to stay above threshold for
    `persistence_windows` consecutive windows before confirming
    an anomaly. Filters out isolated single-window statistical
    blips (e.g. a normal seasonal peak) while still catching real
    anomaly bursts, which persist across many consecutive windows
    since they're 15 timesteps wide and windows slide 1 step at a time.

    One instance per service_id — debounce state (recent windows)
    must not be shared across services, or one service's blip could
    combine with another's to falsely confirm an anomaly.
    """
    def __init__(self, threshold: float, persistence_windows: int = 3):
        self.threshold = threshold
        self.persistence_windows = persistence_windows
        self.recent_flags = deque(maxlen=persistence_windows)

    def check(self, reconstruction_error: float) -> bool:
        """
        Call this once per window, IN TIME ORDER, for a given service.
        Returns True only when the last `persistence_windows` windows
        were ALL above threshold — i.e. a confirmed, non-isolated anomaly.
        Returns False for every window before enough history has built up.
        """
        self.recent_flags.append(reconstruction_error > self.threshold)
        return (
            len(self.recent_flags) == self.persistence_windows
            and all(self.recent_flags)
        )

    def reset(self):
        """Call this if a service restarts or its stream has a gap —
        old windows shouldn't count toward debounce after a discontinuity."""
        self.recent_flags.clear()