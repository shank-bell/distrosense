from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class SpanPayload(BaseModel):
    """
    The one validation gate for every span entering the platform — gRPC and
    REST both go through it (it used to run on the REST path only). Ranges
    mirror what the simulator's generators actually clamp to, so a legitimate
    span is never rejected.
    """
    model_config = ConfigDict(allow_inf_nan=False)

    trace_id:     str   = Field(min_length=1, max_length=64)
    span_id:      str   = Field(min_length=1, max_length=64)
    service_id:   str   = Field(min_length=1, max_length=64)
    timestamp:    int   = Field(gt=0)               # epoch milliseconds
    cpu_percent:  float = Field(ge=0, le=100)
    latency_p99:  float = Field(ge=0)
    error_rate:   float = Field(ge=0, le=1)
    request_rate: float = Field(ge=0)
    anomaly_type: Optional[str] = None
    is_anomaly:   bool  = False
