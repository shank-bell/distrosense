# DistroSense

Real-time distributed anomaly detection platform.

## Stack
- Ingestion: FastAPI + gRPC
- Message bus: Apache Kafka
- Stream processing: Apache Flink
- ML: PyTorch LSTM + TorchServe + Isolation Forest
- Graph: Neo4j
- Storage: TimescaleDB + Elasticsearch + Redis
- Observability: Prometheus + Grafana
- Dashboard: React + TypeScript

## Build order
- Phase 0: Root scaffold (this)
- Phase 1: Simulator
- Phase 2: Kafka + infra bootstrap
- Phase 3: Ingestion service
- Phase 4: Flink jobs
- Phase 5: ML inference service
- Phase 6: Correlation engine
- Phase 7: Response orchestrator
- Phase 8: Dashboard + observability

## Running locally
cp .env.example .env
docker compose up -d