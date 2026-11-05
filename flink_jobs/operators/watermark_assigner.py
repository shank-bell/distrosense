from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner

from flink_jobs.config import SOURCE_IDLENESS_SECONDS, WATERMARK_LAG_SECONDS


class SpanTimestampAssigner(TimestampAssigner):
    """Event time = the span's own timestamp (ms since epoch), not Kafka's append time."""

    def extract_timestamp(self, value, record_timestamp) -> int:
        return value.timestamp


def build_watermark_strategy() -> WatermarkStrategy:
    """
    LLD: BoundedOutOfOrderness(5s) — the watermark trails the highest timestamp
    seen by 5 seconds, so a window closes 5s after its end and anything later
    than that is late (the window operators send it to a side output).

    with_idleness: a parallel instance that receives no events for a while stops
    holding the watermark back, otherwise one idle Kafka partition would stall
    every window.
    """
    return (WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_LAG_SECONDS))
            .with_timestamp_assigner(SpanTimestampAssigner())
            .with_idleness(Duration.of_seconds(SOURCE_IDLENESS_SECONDS)))
