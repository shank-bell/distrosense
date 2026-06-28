from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok", "service": "ingestion"}

@router.get("/ready")
def ready():
    return {"status": "ready"}