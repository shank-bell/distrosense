from pyflink.common import Time, Types
from pyflink.datastream import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows

from flink_jobs.config import TOPIC_AGGREGATED, TUMBLING_WINDOW_SECONDS, TXN_PREFIX_AGGREGATED
from flink_jobs.operators.window_aggregator import MetricAggregationWindow
from flink_jobs.sinks.kafka_sink import build_kafka_sink

LATE_TUMBLING = OutputTag("late-tumbling", Types.PICKLED_BYTE_ARRAY())


class MetricAggregationJob:
    """
    LLD: TumblingEventTimeWindows(60s)
    ProcessWindowFunction: mean, p99, max per service
    Output: MetricAggregate -> TimescaleDB (inside the window function) +
            aggregated-metrics topic (exactly-once Kafka sink)
    """

    @staticmethod
    def attach(keyed_stream):
        """Wire this branch onto the keyBy(service_id) stream; returns the late-event side output."""
        windowed = (keyed_stream
                    .window(TumblingEventTimeWindows.of(Time.seconds(TUMBLING_WINDOW_SECONDS)))
                    .side_output_late_data(LATE_TUMBLING))
        aggregates = (windowed
                      .process(MetricAggregationWindow(), output_type=Types.STRING())
                      .name("tumbling-60s-aggregate").uid("tumbling-60s-aggregate"))
        (aggregates
         .sink_to(build_kafka_sink(TOPIC_AGGREGATED, TXN_PREFIX_AGGREGATED))
         .name("kafka-aggregated-metrics").uid("kafka-aggregated-metrics"))
        return aggregates.get_side_output(LATE_TUMBLING)
