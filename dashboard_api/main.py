import asyncio
import time
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import redis
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from neo4j import GraphDatabase

from dashboard_api import config as C
from dashboard_api.hub import Hub
from dashboard_api.kafka_feed import KafkaFeed

hub = Hub(C.WS_BACKFILL)
_redis = redis.Redis(host=C.REDIS_HOST, port=C.REDIS_PORT, socket_timeout=1, socket_connect_timeout=1)
_services_cache = {"at": 0.0, "data": []}


@asynccontextmanager
async def lifespan(app: FastAPI):
    feed = KafkaFeed(asyncio.get_running_loop(), hub)
    feed.start()
    app.state.feed = feed
    yield
    feed.stop()


app = FastAPI(title="DistroSense Dashboard API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=C.CORS_ORIGINS, allow_methods=["GET"], allow_headers=["*"])


@contextmanager
def db():
    conn = psycopg2.connect(host=C.TIMESCALE_HOST, port=C.TIMESCALE_PORT, dbname=C.TIMESCALE_DB,
                            user=C.TIMESCALE_USER, password=C.TIMESCALE_PASSWORD, connect_timeout=3)
    try:
        yield conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    finally:
        conn.rollback()
        conn.close()


def unavailable(what: str, e: Exception):
    raise HTTPException(status_code=503, detail=f"{what} unavailable: {type(e).__name__}: {e}")


@app.get("/health")
def health():
    return {"status": "ok", "kafka_feed": app.state.feed.connected, "ws_clients": hub.client_count}


@app.websocket("/ws")
async def live(ws: WebSocket):
    await hub.connect(ws)
    try:
        while True:
            await ws.receive_text()          # the browser sends nothing; this just detects disconnects
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)


@app.get("/api/stats")
def stats():
    out: dict = {}
    try:
        with db() as cur:
            cur.execute("SELECT count(*) AS n FROM anomaly_events WHERE detected_at > now() - interval '15 minutes'")
            out["anomalies_15m"] = cur.fetchone()["n"]
            cur.execute("SELECT count(*) AS n FROM incidents WHERE opened_at > now() - interval '1 hour'")
            out["incidents_1h"] = cur.fetchone()["n"]
            cur.execute("SELECT anomaly_type, count(*) AS n FROM anomaly_events "
                        "WHERE detected_at > now() - interval '15 minutes' GROUP BY 1 ORDER BY 2 DESC")
            out["anomalies_by_type"] = {r["anomaly_type"]: r["n"] for r in cur.fetchall()}
            cur.execute("SELECT service_id, count(*) AS n FROM anomaly_events "
                        "WHERE detected_at > now() - interval '15 minutes' GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
            top = cur.fetchone()
            out["noisiest_service"] = top["service_id"] if top else None
    except Exception as e:
        unavailable("TimescaleDB", e)
    try:
        out["topk_size"] = _redis.zcard(C.ANOMALY_TOPK_KEY)
    except Exception:
        out["topk_size"] = None
    try:
        out["logs_indexed"] = requests.get(f"{C.ES_HOST}/logs-*/_count", timeout=2).json().get("count")
    except Exception:
        out["logs_indexed"] = None
    return out


@app.get("/api/incidents")
def incidents(limit: int = Query(20, ge=1, le=200)):
    try:
        with db() as cur:
            cur.execute("SELECT id::text, extract(epoch FROM opened_at) AS opened_at, extract(epoch FROM closed_at) AS closed_at, "
                        "root_cause_service_id, member_service_ids, anomaly_count, severity, causality_method "
                        "FROM incidents ORDER BY opened_at DESC LIMIT %s", (limit,))
            return cur.fetchall()
    except Exception as e:
        unavailable("TimescaleDB", e)


@app.get("/api/anomalies/recent")
def recent_anomalies(limit: int = Query(50, ge=1, le=500)):
    try:
        with db() as cur:
            cur.execute("SELECT id::text, extract(epoch FROM detected_at) * 1000 AS detected_at, service_id, anomaly_type, "
                        "severity, model_used FROM anomaly_events ORDER BY detected_at DESC LIMIT %s", (limit,))
            return cur.fetchall()
    except Exception as e:
        unavailable("TimescaleDB", e)


@app.get("/api/top-anomalies")
def top_anomalies(limit: int = Query(10, ge=1, le=100)):
    """The Redis sorted set Flink maintains: worst anomalies by score."""
    try:
        rows = _redis.zrevrange(C.ANOMALY_TOPK_KEY, 0, limit - 1, withscores=True)
    except Exception as e:
        unavailable("Redis", e)
    out = []
    for member, score in rows:
        service_id, anomaly_type, detected_at = member.decode().split("|")
        out.append({"service_id": service_id, "anomaly_type": anomaly_type,
                    "detected_at": int(detected_at), "anomaly_score": round(score, 4)})
    return out


@app.get("/api/logs")
def logs(service_id: Optional[str] = None, level: Optional[str] = None, q: Optional[str] = None,
         limit: int = Query(50, ge=1, le=200)):
    filters = []
    if service_id:
        filters.append({"term": {"service_id": service_id}})
    if level:
        filters.append({"term": {"level": level.upper()}})
    query = {"bool": {"filter": filters, "must": [{"match": {"message": q}}] if q else [{"match_all": {}}]}}
    try:
        r = requests.post(f"{C.ES_HOST}/logs-*/_search", timeout=5,
                          json={"size": limit, "sort": [{"timestamp": "desc"}], "query": query})
        r.raise_for_status()
    except Exception as e:
        unavailable("Elasticsearch", e)
    return [h["_source"] for h in r.json()["hits"]["hits"]]


@app.get("/api/services")
def services():
    """service_id -> name/tier/team from the Neo4j topology (cached 5 min), so the UI can show names."""
    if time.time() - _services_cache["at"] < 300 and _services_cache["data"]:
        return _services_cache["data"]
    try:
        driver = GraphDatabase.driver(C.NEO4J_URI, auth=(C.NEO4J_USER, C.NEO4J_PASSWORD))
        with driver.session() as s:
            data = [dict(r) for r in s.run("MATCH (s:Service) RETURN s.id AS id, s.name AS name, s.tier AS tier, s.team AS team")]
        driver.close()
    except Exception as e:
        unavailable("Neo4j", e)
    _services_cache.update(at=time.time(), data=data)
    return data


# Serve the built React app from the same process (no CORS, one command for the demo).
_dist = Path(__file__).resolve().parent.parent / "dashboard" / "dist"
if _dist.is_dir():
    app.mount("/", StaticFiles(directory=_dist, html=True), name="ui")


if __name__ == "__main__":
    uvicorn.run(app, host=C.DASHBOARD_HOST, port=C.DASHBOARD_PORT)
