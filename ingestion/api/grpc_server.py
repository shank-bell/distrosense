import grpc
from concurrent import futures
from ingestion.proto import telemetry_pb2, telemetry_pb2_grpc
from ingestion.producer.kafka_producer import KafkaSpanProducer
from ingestion.config import GRPC_HOST, GRPC_PORT


class TelemetryServicer(telemetry_pb2_grpc.TelemetryServiceServicer):
    def __init__(self):
        self.producer = KafkaSpanProducer()

    def IngestSpanBatch(self, request, context):
        accepted = 0
        for span in request.spans:
            try:
                record = {
                    "trace_id": span.trace_id,
                    "span_id": span.span_id,
                    "service_id": span.service_id,
                    "timestamp": span.timestamp,
                    "cpu_percent": next((m.value for m in span.metrics if m.name == "cpu_percent"), 0.0),
                    "latency_p99": next((m.value for m in span.metrics if m.name == "latency_p99"), 0.0),
                    "error_rate": next((m.value for m in span.metrics if m.name == "error_rate"), 0.0),
                    "request_rate": next((m.value for m in span.metrics if m.name == "request_rate"), 0.0),
                    "anomaly_type": span.anomaly_type or None,
                    "is_anomaly": span.is_anomaly,
                }
                self.producer.publish(record, span.service_id)
                accepted += 1
            except Exception as e:
                print(f"[gRPC] Failed to process span {span.span_id}: {e}")

        return telemetry_pb2.IngestResponse(
            success=True,
            spans_accepted=accepted,
            message=f"Accepted {accepted} spans",
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    telemetry_pb2_grpc.add_TelemetryServiceServicer_to_server(
        TelemetryServicer(), server
    )
    server.add_insecure_port(f"{GRPC_HOST}:{GRPC_PORT}")
    server.start()
    print(f"[gRPC] Server listening on {GRPC_HOST}:{GRPC_PORT}")
    server.wait_for_termination()