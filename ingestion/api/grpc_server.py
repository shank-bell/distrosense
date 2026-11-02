import grpc
from collections import Counter
from concurrent import futures
from ingestion import metrics as M
from ingestion.proto import telemetry_pb2, telemetry_pb2_grpc
from ingestion.config import GRPC_HOST, GRPC_PORT


def span_to_dict(span) -> dict:
    """proto Span -> the dict SpanPayload validates. A missing metric stays missing
    (and is rejected) rather than silently defaulting to 0.0."""
    d = {
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "service_id": span.service_id,
        "timestamp": span.timestamp,
        "anomaly_type": span.anomaly_type or None,
        "is_anomaly": span.is_anomaly,
    }
    for m in span.metrics:
        d[m.name] = m.value
    return d


class TelemetryServicer(telemetry_pb2_grpc.TelemetryServiceServicer):
    def __init__(self, pipeline):
        self._pipeline = pipeline

    def IngestSpanBatch(self, request, context):
        with M.BATCH_SECONDS.time():
            outcomes = Counter(self._pipeline.ingest(span_to_dict(s)) for s in request.spans)
        return telemetry_pb2.IngestResponse(
            success=outcomes["rejected"] == 0 and outcomes["failed"] == 0,
            spans_accepted=outcomes["accepted"],
            message=", ".join(f"{k}={v}" for k, v in sorted(outcomes.items())) or "empty batch",
        )


def serve(pipeline):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    telemetry_pb2_grpc.add_TelemetryServiceServicer_to_server(TelemetryServicer(pipeline), server)
    server.add_insecure_port(f"{GRPC_HOST}:{GRPC_PORT}")
    server.start()
    print(f"[gRPC] Server listening on {GRPC_HOST}:{GRPC_PORT}")
    server.wait_for_termination()
