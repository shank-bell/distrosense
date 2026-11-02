from fastapi import APIRouter, HTTPException, Request
from ingestion.models import SpanPayload

router = APIRouter()


@router.post("/ingest")
def ingest_span(payload: SpanPayload, request: Request):
    # FastAPI already validated `payload` (422 on bad input); the pipeline is the
    # same one the gRPC path uses, so dedup / Avro / DLQ / log indexing all apply.
    outcome = request.app.state.pipeline.ingest(payload.model_dump())
    if outcome == "failed":
        raise HTTPException(status_code=500, detail="span could not be serialised or delivered (sent to DLQ)")
    return {"success": True, "outcome": outcome}
