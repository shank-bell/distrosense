from pyflink.datastream.functions import FlatMapFunction

from flink_jobs import metrics as M


class CountLateEvents(FlatMapFunction):
    """
    Terminal operator of the windows' late-data side outputs: an event that
    arrives after its window has closed (watermark = max timestamp - 5s) is
    dropped from the window and lands here, where it is only counted.
    """

    def flat_map(self, value):
        M.LATE_DROPPED.labels("window").inc()
        return ()
