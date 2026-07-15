# DistroSense

> Real-time distributed anomaly detection platform — ingests telemetry from simulated microservices, detects anomalies via ML, correlates across a service dependency graph, and triggers automated responses.

![Python](https://img.shields.io/badge/Python-3.11-blue) ![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-black) ![PyTorch](https://img.shields.io/badge/PyTorch-2.2.1-red) ![Neo4j](https://img.shields.io/badge/Neo4j-5.18-green) ![Docker](https://img.shields.io/badge/Docker-Compose-blue)

---

## What is DistroSense?

DistroSense simulates a production microservices environment — 100 services emitting CPU, latency, error rate, and request rate telemetry every second — and applies a full ML-powered anomaly detection pipeline on top of it in real time.

It is built as a portfolio project to demonstrate distributed systems, stream processing, and ML inference skills from scratch.

---

## Architecture

```
Simulator (100 asyncio services)
        ↓ gRPC / proto3
Ingestion Service (FastAPI + gRPC)
        ↓ Avro → Kafka Producer (acks=all, snappy, idempotent)
Apache Kafka (4 topics, 16/8/4/2 partitions)
        ↓ Consumer Group
Flink Jobs (dedup → watermark → tumbling/sliding windows → z-score)
        ↓ gRPC InferenceRequest
ML Inference Service (LSTM Autoencoder + Isolation Forest fallback)
        ↓ anomaly-events topic
Correlation Engine (Neo4j + BFS + Granger causality)
        ↓ incident-alerts topic
Response Orchestrator (YAML rules → k8s HPA + Slack + LLM runbook)
        ↓
Dashboard (React + TypeScript + WebSocket) + Prometheus + Grafana
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Simulation | Python asyncio, NumPy gaussian noise |
| Ingestion | FastAPI, gRPC, proto3, Avro, Confluent Schema Registry |
| Message Bus | Apache Kafka (Confluent 7.6.0), Zookeeper |
| Stream Processing | PyFlink — tumbling 60s, sliding 5min/30s, RocksDB dedup, z-score |
| ML — Primary | PyTorch LSTM Autoencoder (batch×60×4, hidden=64, latent=32) via TorchServe |
| ML — Fallback | scikit-learn Isolation Forest (n=100, contamination=0.05) |
| Graph Correlation | Neo4j 5.18, Cypher 2-hop traversal, Granger causality (maxlag=5) |
| Storage | TimescaleDB (metrics hypertable), Elasticsearch (logs), Redis (Bloom filter + top-k) |
| Observability | Prometheus, Grafana SLO dashboards |
| Dashboard | React + TypeScript + WebSocket |
| Infrastructure | Docker Compose, k8s HPA |

---

## Project Structure

```
distrosense/
├── simulator/               # 100 asyncio microservice simulators
│   ├── services/            # ServiceRegistry, CascadeMap, BaseService
│   ├── generators/          # MetricGenerator (gaussian), AnomalyInjector, TraceGenerator
│   └── publisher/           # KafkaPublisher (Bloom filter dedup)
│
├── ingestion/               # FastAPI + gRPC ingestion service
│   ├── proto/               # telemetry.proto (SpanBatch)
│   ├── serialisation/       # Avro serialiser + span_batch.avsc schema
│   ├── producer/            # Kafka producer (acks=all, idempotent, snappy, linger=5ms)
│   └── api/                 # gRPC server (port 50051) + REST router + health
│
├── flink_jobs/              # Stream processing pipeline
│   ├── jobs/                # AnomalyDetectionJob, MetricAggregationJob, SlidingWindowJob
│   ├── operators/           # DeduplicationFilter, ZScoreTrigger, WatermarkAssigner
│   ├── sinks/               # TimescaleDBSink, KafkaSink
│   └── models/              # MetricEvent, AnomalyEvent dataclasses
│
├── ml_service/              # ML inference gRPC gateway
│   ├── models/              # LSTMAutoencoder, IsolationForest, Threshold (mean+3σ)
│   ├── gateway/             # gRPC gateway (port 50052), TorchServe router
│   ├── training/            # train_lstm.py, train_iforest.py, dataset.py
│   └── torchserve/          # TorchServe config + custom handler
│
├── correlation/             # Neo4j graph + Granger causality (Phase 6)
├── orchestrator/            # YAML rule engine + k8s HPA + Slack (Phase 7)
├── dashboard/               # React + TypeScript + WebSocket (Phase 8)
│
├── infra/
│   ├── kafka/               # topic-init.sh (4 topics), docker-compose.kafka.yml
│   ├── timescaledb/         # init.sql (hypertable + anomaly_events)
│   ├── prometheus/          # prometheus.yml
│   └── grafana/             # datasources + dashboard provisioning
│
└── docker-compose.yml       # Full stack: Kafka, TimescaleDB, Neo4j, ES, Redis, Prometheus, Grafana
```

---

## Key Design Decisions

### Kafka Producer
- `acks=all` — zero data loss, waits for all in-sync replicas
- `enable.idempotence=True` — no duplicate messages on retry
- `compression.type=snappy` — ~50% payload reduction
- `linger.ms=5` — micro-batching for throughput
- `batch.size=65536` — 64KB batch size

### Flink Stream Processing
- `keyBy(service_id)` — routes events to correct TaskSlot
- Bloom filter pre-check before RocksDB state lookup (~70% reduction in lookups)
- `ValueState<Long>` dedup with TTL=10 minutes
- `TumblingEventTimeWindows(60s)` — mean, p99, max per service
- `SlidingEventTimeWindows(5min, slide=30s)` — z-score anomaly detection
- `BoundedOutOfOrderness(5s)` watermark — handles late-arriving events
- Checkpointing every 30s — exactly-once via 2-phase commit

### LSTM Autoencoder
- Input shape: `(batch, 60, 4)` — 60-step sequence, 4 features
- Encoder: 2-layer LSTM hidden=64, latent=32
- Decoder: reconstructs original sequence
- Loss: MSE between input and reconstruction
- Threshold: `mean + 3σ` over validation set
- Anomaly score: `reconstruction_error / threshold`, normalised to [0,1]

### Isolation Forest (Fallback)
- Activates when TorchServe latency > 50ms
- Features: `[mean, std, p95, p99, max, error_rate, req_rate]`
- `n_estimators=100`, `contamination=0.05`
- Score normalised to [0,1] via sigmoid

### Correlation Engine
- Neo4j graph: `(:Service)-[:CALLS {avg_latency, volume}]->(:Service)`
- 2-hop Cypher traversal to find affected services
- BFS connected components for incident clustering
- Granger causality (`maxlag=5`, `p<0.05`) for root cause identification

### Storage
- **TimescaleDB**: `metrics` hypertable (1-day chunks), `anomaly_events` table
- **Neo4j**: Service dependency DAG
- **Elasticsearch**: Log index `{timestamp, service_id, level, message, trace_id, span_id}`
- **Redis**: `ZADD anomaly_scores` (top-k), Bloom filter for span_id dedup

---

## Getting Started

### Prerequisites
- Docker Desktop (with WSL2 on Windows)
- Python 3.11 (Anaconda recommended)
- Git

### Setup

```bash
git clone https://github.com/shank-bell/distrosense.git
cd distrosense

cp .env.example .env
```

### Start infrastructure

```bash
docker compose up -d zookeeper kafka schema-registry kafka-init
docker compose up -d timescaledb neo4j elasticsearch redis prometheus grafana
```

### Install Python dependencies

```bash
pip install aiokafka numpy python-dotenv faker pybloom-live
pip install fastapi uvicorn grpcio grpcio-tools fastavro confluent-kafka
pip install kafka-python scipy psycopg2-binary statsmodels
pip install torch scikit-learn joblib requests
```

### Train ML models

```bash
set KMP_DUPLICATE_LIB_OK=TRUE   # Windows only
python -m ml_service.training.train_iforest
python -m ml_service.training.train_lstm
```

### Run the pipeline

```bash
# Terminal 1 — Ingestion service
python -m ingestion.main

# Terminal 2 — Flink stream processing
python -c "from flink_jobs.jobs.anomaly_detection_job import AnomalyDetectionJob; AnomalyDetectionJob().run()"

# Terminal 3 — ML inference service
python -m ml_service.main

# Terminal 4 — Simulator (starts emitting telemetry)
python -m simulator.main
```

---

## Kafka Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `raw-telemetry` | 16 | Raw spans from simulator |
| `aggregated-metrics` | 8 | Flink tumbling window aggregates |
| `anomaly-events` | 4 | ML-scored anomaly candidates |
| `incident-alerts` | 2 | Correlated incidents with root cause |

---

## Build Progress

- [x] Phase 0 — Root scaffold + Docker Compose
- [x] Phase 1 — Simulator (100 asyncio services, gaussian noise, anomaly injection, cascade map)
- [x] Phase 2 — Kafka (4 topics, Schema Registry, topic-init)
- [x] Phase 3 — Ingestion (FastAPI + gRPC + Avro + Kafka producer)
- [x] Phase 4 — Flink stream processing (dedup, watermark, tumbling/sliding windows, z-score)
- [x] Phase 5 — ML inference (LSTM autoencoder + Isolation Forest + TorchServe + gRPC gateway)
- [ ] Phase 6 — Correlation engine (Neo4j + BFS + Granger causality)
- [ ] Phase 7 — Response orchestrator (YAML rules + k8s HPA + Slack + LLM runbook)
- [ ] Phase 8 — Dashboard (React + TypeScript + WebSocket + Prometheus + Grafana)

---

## Data Flow (Detailed)

```
1. Simulator emits span every 1s per service
   → gaussian noise on CPU/latency/error_rate/request_rate
   → 5% chance of anomaly injection (CPU_SPIKE, LATENCY_BURST, ERROR_RATE_SPIKE, REQUEST_DROP)
   → cascade map degrades downstream services on anomaly

2. Ingestion receives span via gRPC (proto3 SpanBatch)
   → validates with Pydantic
   → Avro serialises with Schema Registry
   → Kafka producer publishes to raw-telemetry (key=service_id)
   → failed sends routed to dead-letter-queue

3. Flink consumes raw-telemetry
   → Bloom filter + RocksDB dedup (TTL=10min)
   → BoundedOutOfOrderness(5s) watermark drops late events
   → keyBy(service_id) routes to window bucket
   → TumblingWindow(60s): mean/p99/max → TimescaleDB + aggregated-metrics
   → SlidingWindow(5min/30s): z-score per metric → if z>3.0 emit AnomalyEvent

4. AnomalyEvent written to anomaly-events Kafka topic + TimescaleDB

5. ML service receives InferenceRequest via gRPC
   → tries TorchServe LSTM (timeout=50ms)
   → falls back to local LSTM or Isolation Forest
   → returns anomaly_score [0,1], is_anomaly (score>0.7), model_used

6. Correlation engine consumes anomaly-events
   → Neo4j 2-hop Cypher traversal
   → BFS incident clustering
   → Granger causality for root cause

7. Response orchestrator consumes incident-alerts
   → YAML rule matching by severity + type
   → k8s HPA scale-up / Slack alert / LLM runbook

8. Dashboard streams anomaly feed via WebSocket
   → Grafana SLO dashboards via Prometheus
```

---
