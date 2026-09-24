import pandas as pd
import psycopg2
from statsmodels.tsa.stattools import grangercausalitytests
from correlation.config import (
    TIMESCALE_HOST, TIMESCALE_PORT, TIMESCALE_DB, TIMESCALE_USER, TIMESCALE_PASSWORD,
    GRANGER_MAXLAG, GRANGER_PVALUE_THRESHOLD, GRANGER_MIN_POINTS, GRANGER_LOOKBACK_SECONDS,
    ANOMALY_TYPE_TO_METRIC,
)


class GrangerEngine:
    """
    LLD 7.3/7.4: Granger causality (maxlag=5, p<0.05), run pairwise over a
    finalized cluster's member services, to identify the leading indicator
    (root cause).

    For each pair of services in a cluster, tests whether one service's
    recent metric history helps predict the other's (in either direction)
    via statsmodels' grangercausalitytests. A significant result
    (p < GRANGER_PVALUE_THRESHOLD at any lag up to GRANGER_MAXLAG) is
    treated as a directed causal edge A -> B ("A helps predict B"). Root
    cause = the service with the most outgoing edges among services with
    no incoming edges (nothing else in the cluster explains it).

    Falls back to "earliest anomaly in the cluster" when there isn't
    enough accumulated history yet, or when no edges come out significant
    — both are expected early in the system's life, before ~15 minutes of
    continuous per-service metric history has accumulated.
    """

    def __init__(self):
        self._conn = psycopg2.connect(
            host=TIMESCALE_HOST, port=TIMESCALE_PORT, dbname=TIMESCALE_DB,
            user=TIMESCALE_USER, password=TIMESCALE_PASSWORD,
        )

    def close(self):
        self._conn.close()

    def _fetch_series(self, service_id: str, metric_name: str) -> pd.Series:
        query = """
            SELECT time, value FROM metrics
            WHERE service_id = %s AND metric_name = %s
              AND time > now() - %s::interval
            ORDER BY time ASC
        """
        with self._conn.cursor() as cur:
            cur.execute(query, (service_id, metric_name, f"{GRANGER_LOOKBACK_SECONDS} seconds"))
            rows = cur.fetchall()
        if not rows:
            return pd.Series(dtype=float)
        times, values = zip(*rows)
        # Round to the minute: MetricAggregationJob flushes independently per
        # service, so exact-timestamp alignment across services can miss
        # matches on small jitter even though the window cadence is the same.
        idx = pd.to_datetime(list(times)).round("min")
        return pd.Series(values, index=idx).groupby(level=0).mean()

    def _metric_for(self, anomaly_type: str) -> str:
        return ANOMALY_TYPE_TO_METRIC.get(anomaly_type, "cpu_percent")

    def find_root_cause(self, cluster) -> dict:
        latest_by_service = {}
        for a in cluster.anomalies:
            sid = a["service_id"]
            if sid not in latest_by_service or a["detected_at"] > latest_by_service[sid]["detected_at"]:
                latest_by_service[sid] = a

        series_by_service = {}
        for sid, anomaly in latest_by_service.items():
            metric = self._metric_for(anomaly["anomaly_type"])
            s = self._fetch_series(sid, metric)
            if len(s) >= GRANGER_MIN_POINTS:
                series_by_service[sid] = s

        service_ids = list(series_by_service.keys())
        edges = []

        for a_id in service_ids:
            for b_id in service_ids:
                if a_id == b_id:
                    continue
                # statsmodels convention: column 0 is the dependent series,
                # column 1 is the candidate cause. We're testing "does a_id
                # Granger-cause b_id", so b_id goes first.
                merged = pd.concat(
                    [series_by_service[b_id], series_by_service[a_id]],
                    axis=1, join="inner",
                ).dropna()
                if len(merged) < GRANGER_MIN_POINTS:
                    continue
                try:
                    result = grangercausalitytests(merged.values, maxlag=GRANGER_MAXLAG, verbose=False)
                except Exception:
                    continue
                min_p = min(result[lag][0]["ssr_ftest"][1] for lag in result)
                if min_p < GRANGER_PVALUE_THRESHOLD:
                    edges.append((a_id, b_id))

        if not edges:
            earliest = min(cluster.anomalies, key=lambda a: a["detected_at"])
            return {
                "root_cause_service_id": earliest["service_id"],
                "method": "fallback_earliest",
                "causal_edges": [],
            }

        out_degree = {sid: 0 for sid in service_ids}
        in_degree = {sid: 0 for sid in service_ids}
        for a_id, b_id in edges:
            out_degree[a_id] += 1
            in_degree[b_id] += 1

        sources = [sid for sid in service_ids if in_degree.get(sid, 0) == 0]
        candidates = sources if sources else service_ids
        root_cause = max(candidates, key=lambda sid: out_degree.get(sid, 0))

        return {
            "root_cause_service_id": root_cause,
            "method": "granger",
            "causal_edges": edges,
        }