import time
from typing import Dict, List, Set
from correlation.graph.neo4j_client import Neo4jClient
from correlation.config import CORRELATION_HOPS, CORRELATION_WINDOW_SECONDS, CORRELATION_MAX_CLUSTER_AGE_SECONDS


class Cluster:
    """
    One in-progress incident candidate: a set of anomaly events whose
    services are within CORRELATION_HOPS of each other, all detected
    within CORRELATION_WINDOW_SECONDS of one another.
    """
    def __init__(self, anomaly: dict):
        self.anomalies: List[dict] = [anomaly]
        self.service_ids: Set[str] = {anomaly["service_id"]}
        detected_at = anomaly["detected_at"] / 1000.0
        self.created_at: float = detected_at
        self.last_updated: float = detected_at

    def add(self, anomaly: dict):
        self.anomalies.append(anomaly)
        self.service_ids.add(anomaly["service_id"])
        self.last_updated = max(self.last_updated, anomaly["detected_at"] / 1000.0)

    def merge(self, other: "Cluster"):
        self.anomalies.extend(other.anomalies)
        self.service_ids |= other.service_ids
        self.created_at = min(self.created_at, other.created_at)
        self.last_updated = max(self.last_updated, other.last_updated)


class IncidentClusterer:
    """
    LLD: Neo4j 2-hop Cypher traversal + BFS connected components for
    incident clustering. For each new anomaly, its 2-hop neighborhood
    (cached per service, since the topology is static) is checked
    against every open cluster's service set. Overlapping clusters
    within CORRELATION_WINDOW_SECONDS of each other merge into one.

    A cluster finalizes when EITHER:
      - it's gone idle for CORRELATION_WINDOW_SECONDS (no new anomalies
        landing in it), or
      - it's been open for CORRELATION_MAX_CLUSTER_AGE_SECONDS regardless
        of activity.
    The second condition matters in practice: on a dense graph with
    anomalies firing every second or two, a broad 2-hop neighborhood
    means a cluster keeps absorbing new anomalies indefinitely and
    would otherwise never go idle long enough to close.
    """

    def __init__(self, neo4j_client: Neo4jClient):
        self._neo4j = neo4j_client
        self._related_cache: Dict[str, Set[str]] = {}
        self._active_clusters: List[Cluster] = []

    def _related(self, service_id: str) -> Set[str]:
        if service_id not in self._related_cache:
            related = set(self._neo4j.get_related_services(service_id, hops=CORRELATION_HOPS))
            related.add(service_id)
            self._related_cache[service_id] = related
        return self._related_cache[service_id]

    def add_anomaly(self, anomaly: dict) -> None:
        now = anomaly["detected_at"] / 1000.0
        neighborhood = self._related(anomaly["service_id"])

        matching = [
            c for c in self._active_clusters
            if (now - c.last_updated) <= CORRELATION_WINDOW_SECONDS
            and (now - c.created_at) < CORRELATION_MAX_CLUSTER_AGE_SECONDS
            and (c.service_ids & neighborhood)
        ]

        if not matching:
            self._active_clusters.append(Cluster(anomaly))
            return

        target = matching[0]
        target.add(anomaly)
        for other in matching[1:]:
            target.merge(other)
            self._active_clusters.remove(other)

    def pop_finalized(self) -> List[Cluster]:
        """
        Call periodically. Returns clusters that are either idle past
        CORRELATION_WINDOW_SECONDS or older than
        CORRELATION_MAX_CLUSTER_AGE_SECONDS, and removes them from the
        active set — ready for Granger causality next.
        """
        now = time.time()
        finalized = [
            c for c in self._active_clusters
            if (now - c.last_updated) > CORRELATION_WINDOW_SECONDS
            or (now - c.created_at) > CORRELATION_MAX_CLUSTER_AGE_SECONDS
        ]
        for c in finalized:
            self._active_clusters.remove(c)
        return finalized