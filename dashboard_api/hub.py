import json
from collections import deque
from fastapi import WebSocket


class Hub:
    """Fan-out of live events to every connected browser, with a small replay buffer."""

    def __init__(self, backfill: int = 50):
        self._clients: set = set()
        self._recent: deque = deque(maxlen=backfill)

    @property
    def client_count(self) -> int:
        return len(self._clients)

    async def connect(self, ws: WebSocket):
        await ws.accept()
        # Register first so events broadcast during the replay aren't missed, then
        # replay. A browser that vanishes mid-replay is dropped quietly instead of
        # raising out of the WebSocket handler and leaving a dead socket registered.
        self._clients.add(ws)
        try:
            for frame in list(self._recent):
                await ws.send_text(frame)
        except Exception:
            self.disconnect(ws)

    def disconnect(self, ws: WebSocket):
        self._clients.discard(ws)

    async def broadcast(self, frame_type: str, data: dict):
        frame = json.dumps({"type": frame_type, "data": data})
        self._recent.append(frame)
        dead = []
        for ws in list(self._clients):
            try:
                await ws.send_text(frame)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)
