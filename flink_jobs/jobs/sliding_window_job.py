from pyflink.common import Time, Types
from pyflink.datastream import OutputTag
from pyflink.datastream.window import SlidingEventTimeWindows

from flink_jobs.config import (
    SLIDE_INTERVAL_SECONDS, SLIDING_WINDOW_SECONDS, TOPIC_ANOMALY_EVENTS, TXN_PREFIX_ANOMALIES,
)
from flink_jobs.operators.zscore_trigger import ZScoreWindowFunction
from flink_jobs.sinks.kafka_sink import build_kafka_sink

LATE_SLIDING = OutputTag("late-sliding", Types.PICKLED_BYTE_ARRAY())


class SlidingWindowJob:
    """
    LLD: SlidingEventTimeWindows(5min, slide=30s)
    ProcessWindowFunction: z = (value - mean) / (std + eps); |z| > 3.0 -> AnomalyEvent
    Output: anomaly-events topic (exactly-once Kafka sink); TimescaleDB and the Redis
            top-k are written inside the window function.
    """

    @staticmethod
    def attach(keyed_stream):
        """Wire this branch onto the keyBy(service_id) stream; returns the late-event side output."""
        windowed = (keyed_stream
                    .window(SlidingEventTimeWindows.of(Time.seconds(SLIDING_WINDOW_SECONDS),
                                                       Time.seconds(SLIDE_INTERVAL_SECONDS)))
                    .side_output_late_data(LATE_SLIDING))
        anomalies = (windowed
                     .process(ZScoreWindowFunction(), output_type=Types.STRING())
                     .name("sliding-5m-30s-zscore").uid("sliding-5m-30s-zscore"))
        (anomalies
         .sink_to(build_kafka_sink(TOPIC_ANOMALY_EVENTS, TXN_PREFIX_ANOMALIES))
         .name("kafka-anomaly-events").uid("kafka-anomaly-events"))
        return anomalies.get_side_output(LATE_SLIDING)
