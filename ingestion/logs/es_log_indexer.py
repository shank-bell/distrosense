import datetime
import json
import queue
import threading
import time

import requests

from ingestion import metrics as M

INDEX_TEMPLATE = {
    "index_patterns": ["logs-*"],
    "template": {
        "mappings": {
            "properties": {
                "timestamp":  {"type": "date", "format": "epoch_millis"},
                "service_id": {"type": "keyword"},
                "level":      {"type": "keyword"},
                "message":    {"type": "text"},
                "trace_id":   {"type": "keyword"},
                "span_id":    {"type": "keyword"},
            }
        }
    },
}


def span_to_log_doc(span) -> dict:
    """
    README log index: {timestamp, service_id, level, message, trace_id, span_id}.
    The simulator has no log stream of its own, so a log line is derived from
    each span: WARN/ERROR for injected anomalies, INFO otherwise.
    """
    level = "INFO"
    if span.is_anomaly:
        level = "ERROR" if span.anomaly_type == "ERROR_RATE_SPIKE" else "WARN"
    message = (f"span {span.span_id}: cpu={span.cpu_percent:.1f}% p99={span.latency_p99:.0f}ms "
               f"err={span.error_rate:.4f} rps={span.request_rate:.0f}")
    if span.anomaly_type:
        message += f" anomaly={span.anomaly_type}"
    return {"timestamp": span.timestamp, "service_id": span.service_id, "level": level,
            "message": message, "trace_id": span.trace_id, "span_id": span.span_id}


class ElasticsearchLogIndexer:
    """
    Bulk-indexes log documents into daily `logs-YYYY.MM.DD` indices from a
    background thread. Elasticsearch being slow or down must never stall
    ingestion: the queue is bounded, and overflow is dropped and counted.
    """

    def __init__(self, es_url: str, batch_size: int = 500, flush_interval: float = 1.0,
                 max_queue: int = 20_000):
        self._url = es_url.rstrip("/")
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: "queue.Queue[dict]" = queue.Queue(maxsize=max_queue)
        self._http = requests.Session()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="es-log-indexer", daemon=True)
        self._last_error_log = 0.0

    def start(self):
        try:
            r = self._http.put(f"{self._url}/_index_template/distrosense-logs",
                               json=INDEX_TEMPLATE, timeout=5)
            r.raise_for_status()
        except Exception as e:
            print(f"[ES] Could not install the log index template ({e}) — will keep retrying on bulk")
        self._thread.start()
        print(f"[ES] Log indexer started -> {self._url}")

    def add(self, span):
        try:
            self._queue.put_nowait(span_to_log_doc(span))
        except queue.Full:
            M.ES_DROPPED.inc()

    def close(self):
        self._stop.set()
        self._thread.join(timeout=10)

    def _run(self):
        while not self._stop.is_set() or not self._queue.empty():
            batch, deadline = [], time.time() + self._flush_interval
            while len(batch) < self._batch_size and time.time() < deadline:
                try:
                    batch.append(self._queue.get(timeout=0.2))
                except queue.Empty:
                    if self._stop.is_set():
                        break
            if batch:
                self._bulk(batch)

    def _bulk(self, docs):
        lines = []
        for d in docs:
            day = datetime.datetime.fromtimestamp(d["timestamp"] / 1000, datetime.timezone.utc).strftime("%Y.%m.%d")
            lines.append(json.dumps({"index": {"_index": f"logs-{day}"}}))
            lines.append(json.dumps(d))
        try:
            r = self._http.post(f"{self._url}/_bulk", data=("\n".join(lines) + "\n").encode(),
                                headers={"Content-Type": "application/x-ndjson"}, timeout=10)
            r.raise_for_status()
            body = r.json()
            failed = sum(1 for item in body.get("items", []) if item.get("index", {}).get("error"))
            M.ES_INDEXED.inc(len(docs) - failed)
            if failed:
                M.ES_FAILED.inc(failed)
                self._log(f"{failed}/{len(docs)} docs rejected by Elasticsearch")
        except Exception as e:
            M.ES_FAILED.inc(len(docs))
            self._log(f"bulk of {len(docs)} docs failed: {type(e).__name__}: {e}")

    def _log(self, msg: str):
        now = time.time()
        if now - self._last_error_log > 30:      # at most one line per 30s
            self._last_error_log = now
            print(f"[ES] {msg}")
