from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ingestion.producer.kafka_producer import KafkaSpanProducer

router = APIRouter()
producer = KafkaSpanProducer()


class SpanPayload(BaseModel):
    trace_id: str
    span_id: str
    service_id: str
    timestamp: int
    cpu_percent: float
    latency_p99: float
    error_rate: float
    request_rate: float
    anomaly_type: Optional[str] = None
    is_anomaly: bool = False


@router.post("/ingest")
def ingest_span(payload: SpanPayload):
    try:
        record = payload.model_dump()
        producer.publish(record, payload.service_id)
        return {"success": True, "message": "Span accepted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))