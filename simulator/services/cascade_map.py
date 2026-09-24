import random
from typing import Dict, List, Optional


def build_cascade_map(service_ids: List[str], seed: Optional[int] = None) -> Dict[str, List[str]]:
    """
    Builds a dependency map: if service A fails,
    these downstream services are also affected.
    Each service has 1-3 downstream dependents.

    seed: if provided, uses a local Random instance so the map is
    reproducible across process restarts. This matters starting Phase 6 —
    the correlation engine's Neo4j graph and the simulator's cascade_map
    must describe the same topology, so the map can no longer be
    regenerated fresh (and different) on every run.
    """
    rng = random.Random(seed) if seed is not None else random
    cascade_map = {}
    for svc_id in service_ids:
        num_dependents = rng.randint(1, 3)
        dependents = rng.sample(
            [s for s in service_ids if s != svc_id],
            k=min(num_dependents, len(service_ids) - 1)
        )
        cascade_map[svc_id] = dependents
    return cascade_map


def get_affected_services(
    root_service_id: str,
    cascade_map: Dict[str, List[str]],
    max_depth: int = 2
) -> List[str]:
    """
    BFS traversal of cascade map to find all services
    affected by a failure at root_service_id.
    """
    affected = []
    visited = set()
    queue = [(root_service_id, 0)]

    while queue:
        current, depth = queue.pop(0)
        if current in visited or depth > max_depth:
            continue
        visited.add(current)
        if current != root_service_id:
            affected.append(current)
        for downstream in cascade_map.get(current, []):
            if downstream not in visited:
                queue.append((downstream, depth + 1))

    return affected