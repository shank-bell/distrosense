import threading
from contextlib import asynccontextmanager
import redis
import uvicorn
from fastapi import FastAPI, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from ingestion.api.rest_router import router as ingest_router
from ingestion.api.health import router as health_router
from ingestion.api.grpc_server import serve as grpc_serve
from ingestion.config import (
    REDIS_HOST, REDIS_PORT, ES_HOST, DEDUP_ENABLED, ES_LOGS_ENABLED, REST_HOST, REST_PORT,
)
from ingestion.dedup.redis_bloom import RedisBloomFilter
from ingestion.logs.es_log_indexer import ElasticsearchLogIndexer
from ingestion.pipeline import IngestPipeline
from ingestion.producer.kafka_producer import KafkaSpanProducer

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # shutdown: drain the log indexer and the Kafka producer
    if getattr(app.state, "log_indexer", None):
        app.state.log_indexer.close()
    if getattr(app.state, "producer", None):
        app.state.producer.close()


app = FastAPI(title="DistroSense Ingestion Service", lifespan=lifespan)
app.include_router(health_router)
app.include_router(ingest_router)


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def build_pipeline() -> IngestPipeline:
    producer = KafkaSpanProducer()

    dedup = None
    if DEDUP_ENABLED:
        try:
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=0.5, socket_connect_timeout=1)
            client.ping()
            dedup = RedisBloomFilter(client)
            print(f"[Ingest] Redis Bloom dedup on ({dedup.num_bits} bits, {dedup.num_hashes} hashes per bucket)")
        except Exception as e:
            print(f"[Ingest] Redis unavailable ({e}) — running WITHOUT span_id dedup")

    log_indexer = None
    if ES_LOGS_ENABLED:
        log_indexer = ElasticsearchLogIndexer(ES_HOST)
        log_indexer.start()

    app.state.producer, app.state.log_indexer = producer, log_indexer
    return IngestPipeline(producer, dedup=dedup, log_indexer=log_indexer)


if __name__ == "__main__":
    app.state.pipeline = build_pipeline()
    threading.Thread(target=grpc_serve, args=(app.state.pipeline,), daemon=True).start()
    uvicorn.run(app, host=REST_HOST, port=REST_PORT)
