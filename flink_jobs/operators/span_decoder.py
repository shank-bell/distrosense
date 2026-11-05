import time

from pyflink.datastream.functions import FlatMapFunction, RuntimeContext

from flink_jobs import metrics as M
from flink_jobs.models.metric_event import MetricEvent
from flink_jobs.resilience import decode_span


class DecodeSpan(FlatMapFunction):
    """
    raw-telemetry record -> MetricEvent.

    The Kafka source hands records over as ISO-8859-1 strings: that charset maps
    every byte 0-255 to exactly one character, so the original bytes come back
    losslessly with .encode("latin-1") — which is what the Avro (Confluent wire
    format, Schema Registry) / JSON decoder needs. (PyFlink has no raw
    byte-array deserialization schema.)

    Records that cannot be decoded are counted and dropped; one bad payload
    must not fail the job.
    """

    def open(self, runtime_context: RuntimeContext):
        # attempt > 0 means Flink restarted this task after a failure. Count once, not per subtask.
        if runtime_context.get_attempt_number() > 0 and runtime_context.get_index_of_this_subtask() == 0:
            M.JOB_RESTARTS.labels("flink-job").inc()

    def flat_map(self, value):
        try:
            decoded = decode_span(value.encode("latin-1"))
        except UnicodeEncodeError:
            M.DECODE_FAILURES.inc()
            return
        if decoded is None:                      # SpanDecoder already logged + counted Avro failures
            return
        event = MetricEvent.from_dict(decoded)
        if not event.service_id or not event.span_id:
            M.DECODE_FAILURES.inc()
            return
        if not event.timestamp:                  # no event time -> use arrival time rather than epoch 0
            event.timestamp = int(time.time() * 1000)
        yield event
