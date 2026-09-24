import json
import time
from kafka import KafkaConsumer
from correlation.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, TOPIC_ANOMALY_EVENTS,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
)
from correlation.graph.neo4j_client import Neo4jClient
from correlation.clustering.incident_clusterer import IncidentClusterer

FINALIZE_CHECK_INTERVAL_SECONDS = 10


def _check_finalized(clusterer):
    for cluster in clusterer.pop_finalized():
        services = {a["service_id"] for a in cluster.anomalies}
        print(f"[CorrelationEngine] Cluster finalized: "
              f"{len(cluster.anomalies)} anomalies across "
              f"{len(services)} services: {sorted(services)}")


def run():
    neo4j_client = Neo4jClient(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    clusterer = IncidentClusterer(neo4j_client)

    consumer = KafkaConsumer(
        TOPIC_ANOMALY_EVENTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        consumer_timeout_ms=1000,
    )

    print("[CorrelationEngine] Listening on anomaly-events...")
    last_check = time.time()
    try:
        while True:
            for msg in consumer:
                anomaly = msg.value
                clusterer.add_anomaly(anomaly)
                print(f"[CorrelationEngine] Anomaly ingested: "
                      f"{anomaly['service_id']} ({anomaly['anomaly_type']})")

                if time.time() - last_check >= FINALIZE_CHECK_INTERVAL_SECONDS:
                    _check_finalized(clusterer)
                    last_check = time.time()

            # consumer went quiet for 1s — also a good moment to check
            _check_finalized(clusterer)
            last_check = time.time()
    except KeyboardInterrupt:
        print("[CorrelationEngine] Shutting down...")
    finally:
        neo4j_client.close()


if __name__ == "__main__":
    run()