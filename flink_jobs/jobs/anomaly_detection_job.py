import threading
from flink_jobs.jobs.metric_aggregation_job import MetricAggregationJob
from flink_jobs.jobs.sliding_window_job import SlidingWindowJob


class AnomalyDetectionJob:
    """
    LLD: Main Flink job entry point.
    Runs both tumbling window aggregation and
    sliding window z-score detection in parallel threads.
    In production Flink these would be parallel task slots.
    """
    def run(self):
        aggregation_job = MetricAggregationJob()
        sliding_job     = SlidingWindowJob()

        t1 = threading.Thread(target=aggregation_job.run, name="TumblingWindowJob", daemon=True)
        t2 = threading.Thread(target=sliding_job.run,     name="SlidingWindowJob",  daemon=True)

        t1.start()
        t2.start()

        print("[AnomalyDetectionJob] Both jobs running. Ctrl+C to stop.")

        try:
            t1.join()
            t2.join()
        except KeyboardInterrupt:
            print("[AnomalyDetectionJob] Shutting down...")