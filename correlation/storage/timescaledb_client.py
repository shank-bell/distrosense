import uuid
import psycopg2
from correlation.config import TIMESCALE_HOST, TIMESCALE_PORT, TIMESCALE_DB, TIMESCALE_USER, TIMESCALE_PASSWORD


class IncidentStore:
    """
    LLD 7.4 step 5: persists a finalized, root-caused cluster as a row in
    `incidents`, and links every member anomaly back to it via
    anomaly_events.correlated_incident_id.
    """

    def __init__(self):
        self._conn = psycopg2.connect(
            host=TIMESCALE_HOST, port=TIMESCALE_PORT, dbname=TIMESCALE_DB,
            user=TIMESCALE_USER, password=TIMESCALE_PASSWORD,
        )
        self._conn.autocommit = False

    def close(self):
        self._conn.close()

    def save_incident(self, cluster, root_cause_result: dict) -> str:
        incident_id = str(uuid.uuid4())
        member_ids = sorted(cluster.service_ids)
        severity = max(a.get("severity", 1) for a in cluster.anomalies)

        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO incidents
                    (id, opened_at, closed_at, root_cause_service_id,
                     member_service_ids, anomaly_count, severity, causality_method)
                VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s)
                """,
                (
                    incident_id,
                    cluster.created_at,
                    cluster.last_updated,
                    root_cause_result["root_cause_service_id"],
                    member_ids,
                    len(cluster.anomalies),
                    severity,
                    root_cause_result["method"],
                ),
            )

            anomaly_ids = [a["id"] for a in cluster.anomalies if a.get("id")]
            if anomaly_ids:
                cur.execute(
                    """
                    UPDATE anomaly_events
                    SET correlated_incident_id = %s
                    WHERE id = ANY(%s::uuid[])
                    """,
                    (incident_id, anomaly_ids),
                )

        self._conn.commit()
        return incident_id