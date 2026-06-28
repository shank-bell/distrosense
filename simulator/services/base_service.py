import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Optional
from simulator.config import SERVICE_TIERS, SERVICE_TEAMS


@dataclass
class ServiceConfig:
    service_id: str
    name: str
    tier: str
    team: str
    base_cpu: float = 30.0
    base_latency: float = 50.0
    base_error_rate: float = 0.01
    base_request_rate: float = 100.0
    is_anomalous: bool = False
    anomaly_type: Optional[str] = None


class BaseService:
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.is_running = False
        self._anomaly_active = False
        self._anomaly_type = None

    def inject_anomaly(self, anomaly_type: str):
        self._anomaly_active = True
        self._anomaly_type = anomaly_type

    def clear_anomaly(self):
        self._anomaly_active = False
        self._anomaly_type = None

    @property
    def service_id(self) -> str:
        return self.config.service_id

    @property
    def tier(self) -> str:
        return self.config.tier

    @property
    def team(self) -> str:
        return self.config.team

    def __repr__(self):
        return f"<Service {self.config.name} tier={self.config.tier}>"