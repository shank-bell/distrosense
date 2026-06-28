import uuid
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AnomalyEvent:
    service_id:           str
    anomaly_type:         str
    severity:             int
    anomaly_score:        float
    model_used:           str
    detected_at:          int   = field(default_factory=lambda: int(time.time() * 1000))
    id:                   str   = field(default_factory=lambda: str(uuid.uuid4()))
    reconstruction_error: Optional[float] = None
    z_score:              Optional[float] = None
    trace_id:             Optional[str]   = None
    span_id:              Optional[str]   = None

    def to_dict(self) -> dict:
        return {
            "id":                   self.id,
            "service_id":           self.service_id,
            "anomaly_type":         self.anomaly_type,
            "severity":             self.severity,
            "anomaly_score":        self.anomaly_score,
            "model_used":           self.model_used,
            "detected_at":          self.detected_at,
            "reconstruction_error": self.reconstruction_error,
            "z_score":              self.z_score,
            "trace_id":             self.trace_id,
            "span_id":              self.span_id,
        }

    @staticmethod
    def compute_severity(anomaly_score: float) -> int:
        if anomaly_score >= 0.9:  return 5
        if anomaly_score >= 0.75: return 4
        if anomaly_score >= 0.55: return 3
        if anomaly_score >= 0.35: return 2
        return 1