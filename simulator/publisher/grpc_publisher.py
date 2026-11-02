import asyncio
import time
import grpc
from pybloom_live import ScalableBloomFilter
from ingestion.proto import telemetry_pb2, telemetry_pb2_grpc
from simulator import metrics as M
from simulator.config import (
    INGESTION_GRPC_HOST, INGESTION_GRPC_PORT, GRPC_BATCH_MAX, GRPC_BATCH_LINGER_SECONDS,
)

SEND_ATTEMPTS = 3


def to_proto_span(span: dict) -> telemetry_pb2.Span:
    return telemetry_pb2.Span(
        trace_id=span["trace_id"],
        span_id=span["span_id"],
        service_id=span["service_id"],
        timestamp=int(span["timestamp"]),
        metrics=[telemetry_pb2.Metric(name=k, value=float(v)) for k, v in span["metrics"].items()],
        anomaly_type=span.get("anomaly_type") or "",
        is_anomaly=bool(span.get("is_anomaly")),
    )


class GrpcPublisher:
    """
    Same interface as KafkaPublisher (start / publish / stop), but sends spans
    to the ingestion service as gRPC SpanBatch calls — the README's
    simulator -> gRPC -> ingestion path. Spans are buffered and flushed as one
    batch every GRPC_BATCH_LINGER_SECONDS (or when GRPC_BATCH_MAX pile up):
    100 services emitting once a second would otherwise be 100 RPCs/s.
    """

    def __init__(self):
        self._bloom = ScalableBloomFilter(initial_capacity=10000, error_rate=0.001)
        self._buffer = []
        self._channel = None
        self._stub = None
        self._flusher = None
        self._sent = self._accepted = self._dropped = self._duplicates = 0
        self._last_error_log = 0.0

    async def start(self):
        target = f"{INGESTION_GRPC_HOST}:{INGESTION_GRPC_PORT}"
        self._channel = grpc.aio.insecure_channel(target)
        try:
            await asyncio.wait_for(self._channel.channel_ready(), timeout=10)
        except asyncio.TimeoutError:
            await self._channel.close()
            raise RuntimeError(
                f"Ingestion service not reachable at {target}. Start it with "
                f"`python -m ingestion.main`, or run the simulator with SIM_PUBLISH_MODE=kafka."
            )
        self._stub = telemetry_pb2_grpc.TelemetryServiceStub(self._channel)
        self._flusher = asyncio.create_task(self._flush_loop())
        print(f"[GrpcPublisher] Connected to ingestion at {target}")

    async def stop(self):
        if self._flusher:
            self._flusher.cancel()
            await asyncio.gather(self._flusher, return_exceptions=True)
        await self._flush()
        if self._channel:
            await self._channel.close()
        print(f"[GrpcPublisher] Final tally: {self._sent} sent, {self._accepted} accepted by ingestion, "
              f"{self._dropped} dropped, {self._duplicates} flagged as duplicate")

    async def publish(self, span: dict, service_id: str):
        if self._stub is None:
            raise RuntimeError("Publisher not started. Call start() first.")

        span_id = span.get("span_id")
        if span_id and span_id in self._bloom:
            self._duplicates += 1
            M.SPANS_DUPLICATE.inc()
            return
        if span_id:
            self._bloom.add(span_id)

        self._buffer.append(to_proto_span(span))
        if len(self._buffer) >= GRPC_BATCH_MAX:
            await self._flush()

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(GRPC_BATCH_LINGER_SECONDS)
            await self._flush()

    async def _flush(self):
        batch, self._buffer = self._buffer, []        # atomic swap: no await between read and reset
        if not batch:
            return
        request = telemetry_pb2.SpanBatch(spans=batch)
        for attempt in range(1, SEND_ATTEMPTS + 1):
            started = time.perf_counter()
            try:
                resp = await self._stub.IngestSpanBatch(request, timeout=5)
            except grpc.aio.AioRpcError as e:
                if attempt == SEND_ATTEMPTS:
                    self._dropped += len(batch)
                    M.SPANS_DROPPED.inc(len(batch))
                    self._log_error(f"dropped {len(batch)} spans after {attempt} attempts: {e.code().name}")
                    return
                await asyncio.sleep(0.2 * 2 ** (attempt - 1))
                continue
            M.BATCH_SECONDS.observe(time.perf_counter() - started)
            M.BATCHES_SENT.inc()
            M.SPANS_SENT.inc(len(batch))
            M.SPANS_ACCEPTED.inc(resp.spans_accepted)
            self._sent += len(batch)
            self._accepted += resp.spans_accepted
            if not resp.success:
                self._log_error(f"ingestion reported problems: {resp.message}")
            if self._sent // 1000 != (self._sent - len(batch)) // 1000:
                print(f"[GrpcPublisher] Progress: {self._sent} sent, {self._accepted} accepted, "
                      f"{self._dropped} dropped, {self._duplicates} duplicates")
            return

    def _log_error(self, msg: str):
        now = time.time()
        if now - self._last_error_log > 10:
            self._last_error_log = now
            print(f"[GrpcPublisher] {msg}")
