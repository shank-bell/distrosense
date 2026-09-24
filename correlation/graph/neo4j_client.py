from typing import Dict, List
from neo4j import GraphDatabase


class Neo4jClient:
    """
    LLD: (:Service)-[:CALLS {avg_latency, volume}]->(:Service)
    Service dependency DAG — the graph the correlation engine reasons over.
    """

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def load_topology(self, registry: dict, cascade_map: Dict[str, List[str]]) -> None:
        """
        Pushes the simulator's service registry + cascade_map into Neo4j as
        (:Service)-[:CALLS]->(:Service). MERGE throughout so this is safe to
        re-run on every simulator startup without duplicating nodes/edges —
        important now that cascade_map is seeded (Phase 6 needs the graph
        Neo4j holds to match the graph the simulator is actually using).

        avg_latency/volume are placeholders, not measured traffic: this repo
        has no real cross-service call tracing (spans are single-service
        metric emissions, not parent/child call chains — see trace_generator.py),
        so there's nothing to derive real edge weights from yet. avg_latency
        is seeded from the downstream (callee) service's own base_latency,
        and volume from the upstream (caller) service's base_request_rate —
        both real simulator config values, just not measured call volume.
        This is a known simplification, same as the rest of the topology.
        """
        with self._driver.session() as session:
            session.execute_write(self._merge_topology, registry, cascade_map)

    @staticmethod
    def _merge_topology(tx, registry: dict, cascade_map: Dict[str, List[str]]):
        for svc_id, service in registry.items():
            cfg = service.config
            tx.run(
                """
                MERGE (s:Service {id: $id})
                SET s.name = $name, s.tier = $tier, s.team = $team
                """,
                id=svc_id, name=cfg.name, tier=cfg.tier, team=cfg.team,
            )

        for caller_id, callees in cascade_map.items():
            caller = registry.get(caller_id)
            if caller is None:
                continue
            for callee_id in callees:
                callee = registry.get(callee_id)
                if callee is None:
                    continue
                tx.run(
                    """
                    MATCH (a:Service {id: $caller_id})
                    MATCH (b:Service {id: $callee_id})
                    MERGE (a)-[r:CALLS]->(b)
                    SET r.avg_latency = $avg_latency, r.volume = $volume
                    """,
                    caller_id=caller_id,
                    callee_id=callee_id,
                    avg_latency=callee.config.base_latency,
                    volume=caller.config.base_request_rate,
                )

    def get_related_services(self, service_id: str, hops: int = 2) -> List[str]:
        """
        LLD: 2-hop Cypher traversal to find affected services.
        Undirected on purpose — an anomaly can be caused by an upstream
        dependency or show up in a downstream one, and clustering needs
        both directions to catch either case.
        """
        query = f"""
            MATCH (a:Service {{id: $service_id}})-[:CALLS*1..{hops}]-(b:Service)
            RETURN DISTINCT b.id AS id
        """
        with self._driver.session() as session:
            result = session.run(query, service_id=service_id)
            return [record["id"] for record in result]