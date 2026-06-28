import threading
import uvicorn
from fastapi import FastAPI
from ingestion.api.rest_router import router as ingest_router
from ingestion.api.health import router as health_router
from ingestion.api.grpc_server import serve as grpc_serve

app = FastAPI(title="DistroSense Ingestion Service")
app.include_router(health_router)
app.include_router(ingest_router)


def start_grpc():
    grpc_serve()


if __name__ == "__main__":
    grpc_thread = threading.Thread(target=start_grpc, daemon=True)
    grpc_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)