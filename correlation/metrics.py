from prometheus_client import Counter, Gauge

ANOMALIES_INGESTED = Counter("distrosense_correlation_anomalies_ingested_total", "Anomaly events consumed")
INCIDENTS          = Counter("distrosense_correlation_incidents_total", "Incidents finalized and published", ["method"])
FINALIZE_FAILURES  = Counter("distrosense_correlation_finalize_failures_total", "Clusters that failed to save/publish", ["stage"])
OPEN_CLUSTERS      = Gauge("distrosense_correlation_open_clusters", "Incident clusters currently open")
