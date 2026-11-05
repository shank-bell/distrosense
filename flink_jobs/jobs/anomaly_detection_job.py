# Must be first: prometheus_client has to see PROMETHEUS_MULTIPROC_DIR when it is
# first imported (every module below imports flink_jobs.metrics).
from flink_jobs.runtime import prepare_metrics_dir
prepare_metrics_dir()

from pyflink.common import Types, WatermarkStrategy                                   # noqa: E402
from pyflink.common.serialization import SimpleStringSchema                          # noqa: E402
from pyflink.datastream.connectors.kafka import (                                    # noqa: E402
    KafkaOffsetResetStrategy, KafkaOffsetsInitializer, KafkaSource,
)

from flink_jobs import config as C                                                   # noqa: E402
from flink_jobs.jobs.metric_aggregation_job import MetricAggregationJob              # noqa: E402
from flink_jobs.jobs.sliding_window_job import SlidingWindowJob                      # noqa: E402
from flink_jobs.metrics import serve_metrics                                         # noqa: E402
from flink_jobs.operators.deduplication_filter import DeduplicationFilter            # noqa: E402
from flink_jobs.operators.late_counter import CountLateEvents                        # noqa: E402
from flink_jobs.operators.span_decoder import DecodeSpan                             # noqa: E402
from flink_jobs.operators.watermark_assigner import build_watermark_strategy         # noqa: E402
from flink_jobs.runtime import build_environment                                     # noqa: E402

JOB_NAME = "distrosense-anomaly-detection"


def build_source() -> KafkaSource:
    # committed_offsets: resume from the consumer group's last committed offsets (Flink commits
    # them when a checkpoint completes); a group with none starts at AUTO_OFFSET_RESET.
    reset = (KafkaOffsetResetStrategy.EARLIEST if C.AUTO_OFFSET_RESET == "earliest"
             else KafkaOffsetResetStrategy.LATEST)
    return (KafkaSource.builder()
            .set_bootstrap_servers(C.KAFKA_BOOTSTRAP_SERVERS)
            .set_topics(C.TOPIC_RAW_TELEMETRY)
            .set_group_id(C.KAFKA_GROUP_ID)
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(reset))
            .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))   # bytes -> chars 1:1, see DecodeSpan
            .set_property("partition.discovery.interval.ms", "30000")
            .build())


class AnomalyDetectionJob:
    """
    LLD: the Flink job.

        KafkaSource(raw-telemetry)
          -> decode (Avro via Schema Registry, or JSON)
          -> keyBy(span_id) -> dedup: Bloom pre-check + RocksDB ValueState<Long>, TTL 10 min
          -> BoundedOutOfOrderness(5s) watermarks (event time = span timestamp)
          -> keyBy(service_id)
               |- TumblingEventTimeWindows(60s)        -> mean/p99/max -> TimescaleDB + aggregated-metrics
               '- SlidingEventTimeWindows(5min, 30s)   -> z-score > 3.0 -> TimescaleDB + Redis + anomaly-events

    RocksDB state backend, checkpoint every 30s (exactly-once), two-phase-commit Kafka sinks.
    """

    def build(self, env):
        raw = env.from_source(build_source(), WatermarkStrategy.no_watermarks(),
                              "raw-telemetry", type_info=Types.STRING())

        events = (raw
                  .flat_map(DecodeSpan(), output_type=Types.PICKLED_BYTE_ARRAY())
                  .name("decode-span").uid("decode-span"))

        deduped = (events
                   .key_by(lambda e: e.span_id, key_type=Types.STRING())
                   .process(DeduplicationFilter(), output_type=Types.PICKLED_BYTE_ARRAY())
                   .name("dedup-bloom-rocksdb").uid("dedup-bloom-rocksdb"))

        timestamped = (deduped
                       .assign_timestamps_and_watermarks(build_watermark_strategy())
                       .name("watermarks-5s").uid("watermarks-5s"))

        by_service = timestamped.key_by(lambda e: e.service_id, key_type=Types.STRING())

        late_tumbling = MetricAggregationJob.attach(by_service)
        late_sliding = SlidingWindowJob.attach(by_service)

        (late_tumbling.union(late_sliding)
         .flat_map(CountLateEvents(), output_type=Types.PICKLED_BYTE_ARRAY())
         .name("count-late-events").uid("count-late-events")
         .print())                          # emits nothing; just terminates the branch

    def run(self):
        serve_metrics(C.METRICS_PORT, "flink-jobs")
        env = build_environment()
        self.build(env)
        print(f"[AnomalyDetectionJob] starting {JOB_NAME} (parallelism={C.PARALLELISM}, "
              f"checkpoint every {C.CHECKPOINT_INTERVAL_MS // 1000}s, state in {C.STATE_DIR})")
        env.execute(JOB_NAME)


if __name__ == "__main__":
    AnomalyDetectionJob().run()
