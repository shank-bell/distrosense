import time
from common.kafka_consumer import JsonConsumer
from correlation.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, TOPIC_ANOMALY_EVENTS,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, AUTO_OFFSET_RESET, METRICS_PORT,
)
from correlation import metrics as M
from observability.server import start_metrics_server
from correlation.graph.neo4j_client import Neo4jClient
from correlation.clustering.incident_clusterer import IncidentClusterer
from correlation.causality.granger_engine import GrangerEngine
from correlation.storage.timescaledb_client import IncidentStore
from correlation.publisher.incident_publisher import IncidentPublisher

FINALIZE_CHECK_INTERVAL_SECONDS = 10


def _check_finalized(clusterer, granger, store, publisher):
    for cluster in clusterer.pop_finalized():
        incident_id = None
        try:
            services = sorted(cluster.service_ids)
            root_cause_result = granger.find_root_cause(cluster)
            incident_id = store.save_incident(cluster, root_cause_result)
            publisher.publish(incident_id, cluster, root_cause_result)
            M.INCIDENTS.labels(root_cause_result["method"]).inc()
            print(f"[CorrelationEngine] Incident {incident_id[:8]} saved: "
                  f"{len(cluster.anomalies)} anomalies across {len(services)} services: {services}, "
                  f"root cause={root_cause_result['root_cause_service_id']} "
                  f"(method={root_cause_result['method']})")
        except Exception as e:
            M.FINALIZE_FAILURES.labels("publish" if incident_id else "save").inc()
            # One failing cluster must not crash the engine and throw away every
            # other in-memory cluster. Say which half of save->publish failed.
            where = (f"incident {incident_id[:8]} was saved but NOT published"
                     if incident_id else "cluster dropped before it could be saved")
            print(f"[CorrelationEngine] {where}: {type(e).__name__}: {e}")


def run():
    neo4j_client = Neo4jClient(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    clusterer = IncidentClusterer(neo4j_client)
    granger = GrangerEngine()
    store = IncidentStore()
    publisher = IncidentPublisher()
    publisher.open()

    # read_committed: anomaly-events is written by Flink's exactly-once (transactional) sink,
    # which kafka-python cannot read correctly — see common/kafka_consumer.py.
    consumer = JsonConsumer(
        [TOPIC_ANOMALY_EVENTS],
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        name="CorrelationConsumer",
    )

    start_metrics_server(METRICS_PORT, "correlation")
    M.OPEN_CLUSTERS.set_function(clusterer.open_cluster_count)
    print("[CorrelationEngine] Listening on anomaly-events...")
    last_check = time.time()
    try:
        while True:
            record = consumer.poll(1.0)
            if record is not None:
                anomaly = record.value
                clusterer.add_anomaly(anomaly)
                M.ANOMALIES_INGESTED.inc()
                print(f"[CorrelationEngine] Anomaly ingested: "
                      f"{anomaly['service_id']} ({anomaly['anomaly_type']})")

                if time.time() - last_check >= FINALIZE_CHECK_INTERVAL_SECONDS:
                    _check_finalized(clusterer, granger, store, publisher)
                    last_check = time.time()
            else:
                # consumer went quiet for 1s — also a good moment to check
                _check_finalized(clusterer, granger, store, publisher)
                last_check = time.time()
    except KeyboardInterrupt:
        print("[CorrelationEngine] Shutting down...")
    finally:
        consumer.close()
        neo4j_client.close()
        granger.close()
        store.close()
        publisher.close()


if __name__ == "__main__":
    run()