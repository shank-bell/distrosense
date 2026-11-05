# DistroSense

> Real-time distributed anomaly detection platform — ingests telemetry from simulated microservices, detects anomalies, correlates them across a service dependency graph, and triggers automated responses (Kubernetes scaling / canary rollback, Slack alerts, LLM-written runbooks).

![Python](https://img.shields.io/badge/Python-3.11-blue) ![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-black) ![PyTorch](https://img.shields.io/badge/PyTorch-2.2.1-red) ![Neo4j](https://img.shields.io/badge/Neo4j-5.18-green) ![React](https://img.shields.io/badge/React-18%20%2B%20TypeScript-61dafb) ![Docker](https://img.shields.io/badge/Docker-Compose-blue)

---

## What is DistroSense?

DistroSense simulates a production microservices environment — 100 services emitting CPU, latency, error-rate and request-rate telemetry every second — and runs a full detection-to-response pipeline on top of it in real time: schema-validated ingestion, windowed z-score detection, graph-based incident correlation with Granger root-cause analysis, and an orchestrator that acts on the result.

It is built as a portfolio project to demonstrate distributed-systems, stream-processing and ML-inference skills from scratch. The [Known limitations](#known-limitations--roadmap) section says plainly which parts are simulations.

---

## Architecture

```
Simulator (100 asyncio services; AR(1) + seasonal metrics, injected anomalies, cascades)
        ↓ gRPC  SpanBatch (proto3)
Ingestion Service  (gRPC :50051 + FastAPI :8000)
   Pydantic validation → Redis Bloom-filter dedup → Avro + Schema Registry
        ├─→ idempotent Kafka producer (acks=all) ──→ raw-telemetry      failures → dead-letter-queue
        └─→ Elasticsearch  (one log document per span, logs-YYYY.MM.DD)
        ↓
Flink job  (PyFlink 1.18 DataStream · RocksDB state · checkpoint every 30 s · exactly-once Kafka sinks)
   decode → keyBy(span_id) dedup (Bloom pre-check + ValueState, TTL 10 min)
          → BoundedOutOfOrderness(5 s) watermark → keyBy(service_id)
            ├─ TumblingEventTimeWindows 60 s       → TimescaleDB `metrics` + aggregated-metrics
            └─ SlidingEventTimeWindows 5 min / 30 s → z-score → anomaly-events + TimescaleDB + Redis top-k
        ↓ anomaly-events  (read_committed)
Correlation Engine
   Neo4j 2-hop neighbourhoods → BFS connected components → Granger causality (root cause)
   → TimescaleDB `incidents` + incident-alerts
        ↓ incident-alerts
Response Orchestrator
   YAML rules → k8s HPA scale-up / Argo Rollouts canary abort / Slack alert / LLM runbook

Dashboard API (FastAPI + WebSocket) ← Kafka · Redis · TimescaleDB · Elasticsearch · Neo4j
        ↓
React + TypeScript dashboard (:8090)

Prometheus (/metrics on every stage) → Grafana "DistroSense — Pipeline SLOs"

ML inference service (Transformer autoencoder + Isolation Forest) — standalone gRPC service,
not yet called by the Flink job
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Simulation | Python asyncio, NumPy (AR(1) autocorrelation + seasonality + cross-metric correlation), gRPC client |
| Ingestion | FastAPI, gRPC, proto3, Pydantic v2, Avro (fastavro) + Confluent Schema Registry |
| Message bus | Apache Kafka (Confluent 7.6.0), Zookeeper |
| Stream processing | **Apache Flink 1.18 via PyFlink** (DataStream API, local mini-cluster in Docker): RocksDB state backend, 30 s exactly-once checkpoints, two-phase-commit Kafka sinks, tumbling 60 s / sliding 5 min·30 s event-time windows, bounded-out-of-orderness(5 s) watermark, Bloom + `ValueState` TTL dedup, z-score |
| Graph correlation | Neo4j 5.18 (Cypher 2-hop traversal), BFS connected components, Granger causality (`statsmodels`, maxlag=5) |
| Storage | TimescaleDB (`metrics` hypertable, 1-day chunks; `anomaly_events`; `incidents`), Elasticsearch 8 (logs), Redis 7 (Bloom-filter dedup + top-k) |
| Response | YAML rule engine, Kubernetes + Argo Rollouts, Slack (`slack_sdk`), LLM runbooks (Gemini via `google-genai`, or xAI Grok) |
| ML (standalone) | PyTorch Transformer autoencoder (primary), scikit-learn Isolation Forest (warm-up fallback), gRPC gateway |
| Observability | `prometheus_client`, Prometheus, Grafana (provisioned SLO dashboard) |
| Dashboard | FastAPI + WebSocket backend, React 18 + TypeScript (Vite) frontend |
| Infrastructure | Docker Compose, Docker Desktop Kubernetes |

---

## Project Structure

```
distrosense/
├── simulator/               # 100 asyncio microservice simulators
│   ├── services/            # ServiceRegistry, CascadeMap, BaseService
│   ├── generators/          # MetricGenerator, AnomalyInjector, TraceGenerator
│   └── publisher/           # GrpcPublisher (default, via ingestion) · KafkaPublisher (direct, debug)
│
├── ingestion/               # gRPC + REST ingestion service
│   ├── proto/               # telemetry.proto (SpanBatch) + generated stubs
│   ├── api/                 # gRPC servicer, REST router, health
│   ├── models.py            # Pydantic span validation (shared by gRPC and REST)
│   ├── pipeline.py          # validate → dedup → Kafka → log index
│   ├── dedup/               # Redis-bitmap Bloom filter
│   ├── logs/                # Elasticsearch bulk log indexer
│   ├── producer/            # idempotent Avro producer + dead-letter routing
│   └── serialisation/       # span_batch.avsc + Schema Registry serializer
│
├── flink_jobs/              # PyFlink stream processing (runs in the `flink` compose service)
│   ├── jobs/                # AnomalyDetectionJob (builds + runs the job), MetricAggregationJob, SlidingWindowJob
│   ├── operators/           # DecodeSpan, DeduplicationFilter (Bloom + ValueState), watermark_assigner,
│   │                        #   MetricAggregationWindow, ZScoreTrigger / ZScoreWindowFunction, CountLateEvents
│   ├── sinks/               # Flink KafkaSink (exactly-once), TimescaleDBSink, RedisTopKSink
│   ├── models/              # MetricEvent, AnomalyEvent
│   ├── runtime.py           # execution environment: RocksDB, checkpoints, restart strategy, JARs
│   ├── fetch_jars.py        # pinned + SHA-1-verified Kafka connector JAR (Maven Central)
│   ├── Dockerfile           # Python 3.10 + JRE 11 + PyFlink 1.18 (build context: repo root)
│   └── resilience.py        # Avro/JSON span decoder, throttled error log
│
├── correlation/             # Phase 6: incident correlation
│   ├── graph/               # Neo4j client (topology + 2-hop queries)
│   ├── clustering/          # BFS connected-component incident clustering
│   ├── causality/           # Granger root-cause engine
│   ├── storage/             # incidents → TimescaleDB
│   └── publisher/           # incident-alerts producer
│
├── orchestrator/            # Phase 7: response orchestrator
│   ├── rules.yaml           # first-match rule set
│   ├── rule_engine.py · incident_filter.py (dedup / stale / cooldown)
│   ├── integrations/        # k8s client, Slack client, LLM runbook suggester
│   ├── k8s/                 # Argo Rollout + Service + HPA manifests for 4 real services
│   └── services/demo_service/   # the container those Rollouts run
│
├── dashboard_api/           # FastAPI: WebSocket live feed + REST (Redis, TimescaleDB, ES, Neo4j)
├── dashboard/               # Phase 8: React + TypeScript (Vite) UI
│
├── ml_service/              # standalone ML inference gateway (gRPC :50052)
│   ├── models/              # TransformerAutoencoder, IsolationForest, threshold, debouncer
│   ├── gateway/             # gRPC gateway
│   └── training/            # train_transformer.py, train_iforest.py, dataset generation
│
├── common/                  # JsonProducer (idempotent) + JsonConsumer (read_committed), on confluent-kafka
├── observability/           # Prometheus metrics-server helper
│
├── infra/
│   ├── kafka/               # topic-init.sh (5 topics), docker-compose.kafka.yml
│   ├── timescaledb/         # init.sql (hypertable + anomaly_events + incidents)
│   ├── prometheus/          # prometheus.yml (scrapes every stage)
│   └── grafana/             # datasources + provisioned dashboard
│
├── docker-compose.yml       # Kafka, Zookeeper, Schema Registry, TimescaleDB, Neo4j, Elasticsearch, Redis, Prometheus, Grafana
├── run_flink.py             # runs the Flink job in-process (needs PyFlink + a JDK; normally use the compose service)
└── Makefile                 # compose shortcuts
```

---

## Key Design Decisions

### Ingestion
- **One validation gate.** Every span — gRPC or REST — passes `SpanPayload` (ranges mirror what the generators clamp to). A span with a missing metric is rejected, not silently zero-filled.
- **Redis Bloom-filter dedup.** `span_id`s are checked against a bitmap Bloom filter in Redis (no RedisBloom module needed), so dedup survives restarts and is shared between ingestion instances. The filter is time-bucketed (1 h, previous bucket still consulted, 2 h TTL) so it never saturates; false-positive rate ≈ 0.1 %. If Redis is down, ingestion fails open.
- **Avro + Schema Registry.** Records are written in the Confluent wire format (magic byte + schema id + Avro) under subject `raw-telemetry-value`. Consumers decode with nothing but the registry URL.
- **Dead-letter queue.** A span that cannot be serialised or delivered goes to `dead-letter-queue` with the reason in a header.
- **Elasticsearch log index.** Each accepted span is also indexed as a log document `{timestamp, service_id, level, message, trace_id, span_id}` into daily `logs-YYYY.MM.DD` indices through a bounded, non-blocking bulk indexer (ES being down never stalls ingestion).

### Kafka producers
- Ingestion and the incident publisher use **`confluent-kafka`** with `acks=all`, `enable.idempotence=true`, `linger.ms=5`, `batch.size=64 KB`. (`kafka-python` has no idempotent producer.) Incident publishes wait for the broker ack and raise on failure.
- **Flink writes `anomaly-events` and `aggregated-metrics` exactly-once**: its Kafka sink writes inside a transaction that is committed when a checkpoint completes (every 30 s), so those records become visible up to ~30 s after they are produced. The broker needs `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1` on a single node (set in `docker-compose.yml`).
- **Consumers of those two topics must read `read_committed`.** `kafka-python` has no such mode and hands the transaction-marker records to the application as ordinary messages, so correlation and the dashboard feed use `confluent-kafka` (`common/kafka_consumer.py`). The orchestrator reads `incident-alerts`, which is not transactional, and still uses `kafka-python`.
- **Compression is gzip, not snappy**: the orchestrator's `kafka-python` consumer can only decode gzip without an optional native extension — with snappy it crashed with `UnsupportedCodecError`.
- Replication factor is 1 (single-broker dev setup), so "zero data loss" is not claimed across broker failure.

### Stream processing
**A real Apache Flink job** (PyFlink 1.18, DataStream API) on a local mini-cluster inside the `flink` compose service; Flink Web UI on http://localhost:8082.

- **Source:** `KafkaSource` on `raw-telemetry`, resuming from the group's committed offsets (committed when a checkpoint completes). Records are decoded as **Avro via the Schema Registry, or JSON**, so old data and `SIM_PUBLISH_MODE=kafka` still work.
- **Dedup:** `keyBy(span_id)` → `ValueState<Long>` (first-seen time) with a **10-minute state TTL on the RocksDB backend**. A **Bloom-filter pre-check** sits in front: a span the filter has never seen is provably new, so the state *read* is skipped (counter `distrosense_flink_dedup_bloom_skipped_lookups_total`). The filter is two rotating generations (bounded memory) and is treated as untrusted for the first TTL after a (re)start, because it lives in worker memory while the restored state does not.
- **Event time:** the span's own timestamp, `BoundedOutOfOrderness(5 s)` watermark with idleness handling; events that arrive after their window closed go to a side output and are counted (`late_dropped`).
- **Windows** (`keyBy(service_id)`): `TumblingEventTimeWindows(60 s)` → mean / p99 / max → TimescaleDB + `aggregated-metrics`; `SlidingEventTimeWindows(5 min, slide 30 s)` → z-score of the latest event against the window, `z > 3.0` in the anomalous direction (CPU, latency, error rate **up**; request rate **down**) ⇒ anomaly. Score = `min(1, |z|/10)`; severity 1–5 from score (≥0.35, 0.55, 0.75, 0.9). Only the latest event per window is judged — judging every event multiplies the false-positive rate (~30 % of windows on pure noise) because the cut-off is fixed.
- **Fault tolerance:** checkpoint every 30 s (exactly-once, retained on cancellation), RocksDB incremental snapshots, fixed-delay restart strategy; the Kafka sinks are two-phase-commit transactional.
- **Idempotent side effects:** TimescaleDB and Redis are written from inside the window functions. Flink replays windows after a failure, so every write is idempotent: anomaly ids are deterministic (`uuid5` of service + event time + type), `anomaly_events` has a primary key, and `metrics` has a unique index on `(time, service_id, metric_name)`. A write that still fails after retries fails the task, so Flink restarts it instead of dropping rows.
- Each anomaly is also pushed to a Redis sorted set (`anomaly_scores`, top 100) for the dashboard.
- Metrics: PyFlink runs Python operators in separate worker processes, so Prometheus counters use `prometheus_client` multiprocess mode and the driver serves the sum on `:9101`.

### Correlation engine
- 2-hop Cypher neighbourhoods; **BFS connected components** over the services that currently have an open anomaly merge into one incident cluster.
- A cluster finalizes after 300 s idle or 90 s of age (the cap matters on a dense graph).
- Root cause by **Granger causality** (`maxlag=5`, `p<0.05`, ≥15 aligned points); falls back to the earliest-anomalous service when the test is inconclusive (`method=fallback_earliest`).
- A failing cluster is logged and skipped — it can't take down the engine.

### Response orchestrator
- First-match YAML rules over `severity`, `anomaly_types`, `member_services` (table below).
- **Safe under at-least-once delivery:** reads from `earliest` and commits only after an incident is handled; drops duplicate `incident_id`s, incidents older than 1 h, and repeats for the same root cause within 5 min (a higher severity still gets through).
- **Real k8s actions, honestly scoped:** only 4 of the 100 simulated services have a deployment; actions on the others are logged as skipped, never faked. HPA `minReplicas` is raised and **automatically reverted after 15 min**; a canary abort is sent only when a canary is actually in flight.
- Each action runs independently with timeouts — a failed k8s call never suppresses the Slack alert. With no kubeconfig it runs Slack-only.
- **LLM runbook** from the incident plus the Neo4j neighbourhood (Gemini by default, xAI Grok optional), with a timeout and one retry.

### Storage
- **TimescaleDB:** `metrics` hypertable (1-day chunks), `anomaly_events`, `incidents`.
- **Neo4j:** service dependency graph, pushed by the simulator at startup (`MERGE`, safe to rerun).
- **Elasticsearch:** `logs-YYYY.MM.DD`.
- **Redis:** `bloom:span:<bucket>` (dedup), `anomaly_scores` (top-k).

### ML inference service (standalone)
- **Transformer autoencoder** is the primary model: input `(batch, 60, 4)`, `d_model=32`, 4 heads, 2 encoder layers, feed-forward 64. Threshold `mean + 3σ` over validation reconstruction error; score normalised to [0, 1].
- **Isolation Forest** (n=100, contamination=0.05) covers the warm-up period before a service has a full 60-step buffer.
- An **anomaly debouncer** requires 2 consecutive windows above the threshold before `is_anomaly` is set (score threshold 0.7).
- It is **not on the live path yet**: the stream jobs detect with z-score. Wiring the gateway into the jobs is on the roadmap.

---

## Getting Started

### Prerequisites
- Docker Desktop (WSL2 on Windows)
- Python 3.11 (Anaconda recommended) and Git
- Node.js 18+ (only to build the dashboard UI)
- Optional, for real k8s actions: Docker Desktop's Kubernetes + `kubectl` + Argo Rollouts

### Setup
```bash
git clone https://github.com/shank-bell/distrosense.git
cd distrosense
cp .env.example .env          # then edit .env — it is gitignored; never put real keys in .env.example
```

### Start infrastructure
```bash
docker compose up -d          # Zookeeper, Kafka (+ topic init), Schema Registry, TimescaleDB, Neo4j, Elasticsearch, Redis, Prometheus, Grafana
```

### Install Python dependencies
```bash
conda create -n distrosense python=3.11 -y && conda activate distrosense
pip install -r ingestion/requirments.txt -r simulator/requirements.txt \
            -r correlation/requirements.txt -r orchestrator/requirements.txt -r dashboard_api/requirements.txt
pip install -r ml_service/requirements.txt    # only if you want the standalone ML service
```
(The ingestion requirements file is spelled `requirments.txt` in the repo.) The Flink job has its own pinned environment, **inside its Docker image** — PyFlink pins `numpy<1.25` and `protobuf<4.24`, which conflicts with ingestion's protobuf 4.25.3, so do not install `flink_jobs/requirements.txt` next to the others.

### Run the pipeline
Start the stages in this order (Python stages: each in its own terminal, `conda activate distrosense`):

```bash
python -m ingestion.main         # 1. gRPC :50051 + REST :8000 (needs Kafka, Schema Registry, Redis, ES)
docker compose --profile flink up -d --build flink   # 2. the PyFlink job (first build downloads ~1.5 GB)
python -m correlation.main       # 3. Neo4j clustering + Granger → incident-alerts
python -m orchestrator.main      # 4. rules → k8s / Slack / LLM runbook
python -m simulator.main         # 5. starts emitting telemetry (pushes the topology to Neo4j first)
python -m dashboard_api.main     # 6. dashboard backend + UI on http://localhost:8090
```

The Flink job is behind a compose profile so a plain `docker compose up` does not start it (it settles at ~1.7 GB — JVM heap capped at 1 GB plus the Python process and RocksDB — inside a 2.2 GB container limit). Stop it with `docker compose stop flink`; logs with `docker logs -f flink-jobs`; Web UI at http://localhost:8082. The first anomalies appear ~35–40 s after a window closes plus up to 30 s for the checkpoint that commits them.

To run the job outside Docker you need a JDK 8/11/17 and a separate Python 3.10 environment with `flink_jobs/requirements.txt`, then `python flink_jobs/fetch_jars.py && python run_flink.py`. On a RAM-constrained Windows machine the Docker route is the dependable one: the JVM crashed with out-of-memory on a 15 GB laptop whose Docker VM held 10 GB.

Build the dashboard UI once (the backend serves `dashboard/dist`):
```bash
cd dashboard && npm install && npm run build      # or `npm run dev` for hot reload on :5173
```

`python -m simulator.main` needs the ingestion service. To bypass it (JSON straight to Kafka, for debugging): `SIM_PUBLISH_MODE=kafka`.

Incidents take a few minutes to appear: anomalies are detected over a 5-minute window and a cluster closes after 90 s of age or 300 s idle.

### Where to look
| What | URL |
|---|---|
| Dashboard (live feed, incidents, Redis top-k, log search) | http://localhost:8090 |
| Grafana → *DistroSense — Pipeline SLOs* (default login `admin` / `admin`, local only) | http://localhost:3001 |
| Prometheus targets | http://localhost:9090/targets |
| Neo4j browser | http://localhost:7474 |
| Schema Registry subjects | http://localhost:8081/subjects |

### Kubernetes demo (optional)
Only four services — `payment`, `order`, `inventory`, `auth` — have real workloads; the orchestrator skips k8s actions for the rest.
```bash
docker build -t distrosense/demo-service:latest orchestrator/services/demo_service
# Argo Rollouts (use `create`, not `apply`: its CRDs exceed the last-applied annotation limit)
kubectl --context docker-desktop create namespace argo-rollouts
kubectl --context docker-desktop create -n argo-rollouts -f https://github.com/argoproj/argo-rollouts/releases/latest/download/install.yaml
kubectl --context docker-desktop apply -f orchestrator/k8s/
```
Set `K8S_CONTEXT=docker-desktop` in `.env` so the orchestrator targets that cluster no matter which context is current. Docker Desktop's cluster has no metrics-server, so HPA CPU targets show `<unknown>`; raising `minReplicas` still scales.

### Slack & LLM runbooks (optional)
- **Slack:** create an app with the `chat:write` and `chat:write.public` scopes, install it, put the bot token in `.env` as `SLACK_BOT_TOKEN`, and create the channel named in `SLACK_CHANNEL` (default `#incidents`). Unset, Slack posts are logged and skipped.
- **LLM:** `LLM_PROVIDER=gemini` with a Google AI Studio key (default model `gemini-flash-lite-latest`, a rolling alias — pinned model ids get retired), or `LLM_PROVIDER=xai` with an xAI key. Unset, runbooks are skipped.

### Train the ML models (standalone service)
```bash
set KMP_DUPLICATE_LIB_OK=TRUE         # Windows only
python -m ml_service.training.train_iforest
python -m ml_service.training.train_transformer
python -m ml_service.main             # gRPC gateway on :50052
```
(`train_lstm.py` and the TorchServe handler are legacy from the earlier LSTM design and are not used.)

---

## Ports

| Port | Service |
|---|---|
| 9092 | Kafka |
| 8081 | Schema Registry |
| 5432 | TimescaleDB |
| 7474 / 7687 | Neo4j (browser / bolt) |
| 9200 | Elasticsearch |
| 6379 | Redis |
| 9090 | Prometheus |
| 3001 | Grafana |
| 50051 / 8000 | Ingestion gRPC / REST (+ `/metrics`) |
| 9100 / 9101 / 9102 / 9103 | Metrics: simulator / stream jobs / correlation / orchestrator |
| 8090 | Dashboard API + UI (5173 for `npm run dev`) |
| 50052 | ML gateway (standalone) |

## Configuration

All settings are environment variables (`.env`); everything has a working default for the local stack.

| Variable | Purpose | Default |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SCHEMA_REGISTRY_URL` | Kafka / registry | `localhost:9092`, `http://localhost:8081` |
| `TIMESCALE_*`, `NEO4J_*`, `ES_HOST`, `REDIS_HOST/PORT` | datastores | local compose values |
| `SIM_PUBLISH_MODE` | `grpc` (via ingestion) or `kafka` (direct JSON) | `grpc` |
| `INGEST_DEDUP_ENABLED`, `INGEST_ES_LOGS_ENABLED` | toggle Redis dedup / ES log index | `true` |
| `FLINK_AUTO_OFFSET_RESET` | where a *new* consumer group starts on `raw-telemetry` | `latest` |
| `FLINK_PARALLELISM` | Flink task parallelism (memory scales with it) | `1` |
| `FLINK_CHECKPOINT_INTERVAL_MS` | checkpoint / Kafka-transaction commit interval | `30000` |
| `FLINK_WEB_UI_PORT`, `FLINK_METRICS_PORT` | Flink Web UI / Prometheus endpoint | `8082`, `9101` |
| `CORRELATION_AUTO_OFFSET_RESET` | same, for `anomaly-events` | `earliest` |
| `SLACK_BOT_TOKEN`, `SLACK_CHANNEL` | Slack alerts | unset, `#incidents` |
| `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL` | runbook LLM | inferred from key, unset, `gemini-flash-lite-latest` |
| `K8S_NAMESPACE`, `K8S_CONTEXT` | k8s target | `default`, current context |
| `ORCH_COOLDOWN_SECONDS`, `ORCH_MAX_INCIDENT_AGE_SECONDS`, `HPA_REVERT_AFTER_SECONDS` | alert hygiene | `300`, `3600`, `900` |

`auto_offset_reset` only matters for a consumer group with no committed offsets — a group that already has offsets always resumes from them, so restarts never lose data. The stream jobs default to `latest` on purpose: a brand-new group on `earliest` would replay up to a week of telemetry and write every historic window into `metrics` a second time.

---

## Kafka Topics

| Topic | Partitions | Format | Purpose |
|---|---|---|---|
| `raw-telemetry` | 16 | Avro (Schema Registry) | Validated spans from ingestion |
| `aggregated-metrics` | 8 | JSON | Tumbling-window aggregates |
| `anomaly-events` | 4 | JSON | Detected anomalies |
| `incident-alerts` | 2 | JSON | Correlated incidents with root cause, severity, anomaly types |
| `dead-letter-queue` | 2 | raw bytes + `error` header | Spans that could not be serialised or delivered |

---

## Response Rules (`orchestrator/rules.yaml`)

First match wins, top to bottom.

| Rule | Condition | Actions |
|---|---|---|
| `critical_multi_service_cascade` | severity ≥ 4 and ≥ 3 services | HPA scale-up, canary abort, Slack, LLM runbook |
| `critical_single_service` | severity ≥ 4 | HPA scale-up, Slack, LLM runbook |
| `cpu_saturation` | `CPU_SPIKE` and severity ≥ 3 | HPA scale-up, Slack |
| `moderate_incident` | severity ≥ 2 | Slack |
| `default_low_severity` | anything | Slack |

---

## Observability

Every stage exposes Prometheus metrics (`distrosense_*`): ingestion (spans received / accepted / rejected / duplicate / published / dead-lettered, batch latency, ES indexing), simulator (sent / accepted / dropped), stream jobs (events, drops, windows, anomalies by type, processing errors, **supervisor restarts**), correlation (anomalies, incidents by method, open clusters) and orchestrator (incident outcomes, rule matches, actions by result).

The provisioned Grafana dashboard *DistroSense — Pipeline SLOs* shows ingestion availability (SLO ≥ 99 %), throughput and gRPC latency percentiles, stream-processing rates, anomalies per minute by type, incidents and orchestrator outcomes, an error panel that should stay flat, and TimescaleDB panels for anomalies and the latest incidents.

---

## Data Flow (Detailed)

```
1. Simulator emits one span per service per second
   → autocorrelated (AR(1)) + seasonal metrics with cross-metric correlation
   → 5% chance of an injected anomaly (CPU_SPIKE, LATENCY_BURST, ERROR_RATE_SPIKE, REQUEST_DROP)
   → a cascade map degrades downstream services (CASCADE spans)
   → spans are batched (≤200 or 100 ms) into a gRPC SpanBatch

2. Ingestion validates, de-duplicates, serialises
   → Pydantic validation (rejects bad spans)
   → Redis Bloom-filter dedup on span_id
   → Avro + Schema Registry → raw-telemetry (key = service_id); failures → dead-letter-queue
   → one log document per span → Elasticsearch

3. Stream jobs consume raw-telemetry (Avro or JSON)
   → Bloom + TTL dedup, BoundedOutOfOrderness(5 s) watermark drops late events
   → TumblingWindow(60 s): mean / p99 / max → TimescaleDB + aggregated-metrics
   → SlidingWindow(5 min / 30 s): z-score per metric → z > 3.0 emits an AnomalyEvent

4. AnomalyEvent → anomaly-events topic + TimescaleDB + Redis top-k

5. Correlation engine consumes anomaly-events
   → 2-hop Neo4j neighbourhoods → BFS connected components → incident cluster
   → Granger causality over TimescaleDB history → root cause
   → incident row in TimescaleDB + incident-alerts message

6. Response orchestrator consumes incident-alerts
   → drops duplicates / stale / cooldown repeats
   → first matching rule → HPA scale-up · canary abort · LLM runbook · Slack
   → HPA minReplicas reverted after 15 min

7. Dashboard API tails anomaly-events and incident-alerts and streams them over WebSocket;
   REST endpoints read TimescaleDB, Redis, Elasticsearch and Neo4j.
   Prometheus scrapes every stage; Grafana renders the SLO dashboard.
```

---

## Build Progress

- [x] Phase 0 — Root scaffold + Docker Compose
- [x] Phase 1 — Simulator (100 asyncio services, realistic metrics, anomaly injection, cascade map)
- [x] Phase 2 — Kafka (5 topics, Schema Registry, topic-init)
- [x] Phase 3 — Ingestion (gRPC + REST, Pydantic validation, Redis dedup, Avro, DLQ, Elasticsearch log index)
- [x] Phase 4 — Stream processing: **PyFlink 1.18** (RocksDB dedup with Bloom pre-check, watermark, tumbling/sliding windows, z-score, exactly-once Kafka sinks, checkpointing)
- [x] Phase 5 — ML inference service (Transformer autoencoder + Isolation Forest + gRPC gateway) — *standalone, not yet wired into Phase 4*
- [x] Phase 6 — Correlation engine (Neo4j + BFS + Granger causality)
- [x] Phase 7 — Response orchestrator (YAML rules + k8s HPA/Argo + Slack + LLM runbook)
- [x] Phase 8 — Dashboard & observability (React + TypeScript + WebSocket, Prometheus, Grafana)

---

## Known limitations / roadmap

- **Flink runs as a local mini-cluster** (one JVM hosting the job manager and one task manager), not a distributed cluster. State is on RocksDB with checkpoints on a Docker volume. Task failures recover from the last checkpoint — verified by taking TimescaleDB down across a window boundary: Flink recorded the failure, restored from a checkpoint, replayed the window, and the committed output had no duplicates and no missing minutes. A *fresh start* of the container resumes from the committed Kafka offsets but does not restore the previous run's window state (an in-flight window at the moment of a full restart is lost).
- **Throughput ceiling of the local mode:** PyFlink runs the Python operators inside the driver process (loopback mode), so decode, dedup, windows and sinks share one Python process / one core. At the simulator's ~107 events/s it uses 70–90 % of that core and keeps up with no growing lag; a much higher rate needs a real Flink cluster, where Python workers are separate processes.
- **Kafka records written by Flink have no key** (PyFlink's Kafka sink cannot key by a field of the element), so `anomaly-events` / `aggregated-metrics` are not partitioned by service. Nothing here depends on that.
- **The ML service is not on the live path.** Anomalies currently come from the z-score; the Transformer gateway runs standalone and still needs to be called from the Flink job.
- **Only 4 of 100 simulated services have real Kubernetes workloads.** Most incidents therefore skip the HPA/canary actions (and say so in the log).
- **Single-broker dev setup** (replication factor 1); Redis Bloom dedup has a ~0.1 % false-positive rate by design.
- **Orchestrator state is in memory** (incident filter, HPA revert timers): a restart forgets pending reverts.
- Slack and LLM features need your own bot token / API key; both degrade gracefully when unset.
