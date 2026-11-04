import os
from fastapi import FastAPI

SERVICE_NAME = os.getenv("SERVICE_NAME", "demo-service")
VERSION = os.getenv("VERSION", "v1")

app = FastAPI(title=SERVICE_NAME)


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "version": VERSION}


@app.get("/")
def root():
    return {"service": SERVICE_NAME, "version": VERSION}