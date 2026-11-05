"""
Flink runtime setup: the StreamExecutionEnvironment (state backend, checkpointing,
restart strategy, connector JARs, Python workers) and the shared metrics directory.

This module must stay free of prometheus_client imports: prepare_metrics_dir()
has to run before anything imports it.
"""
import glob
import os
import sys
import tempfile

from flink_jobs import config as C


def prepare_metrics_dir() -> str:
    """
    Point prometheus_client at a shared directory (multiprocess mode) BEFORE it is
    imported. The JVM inherits this variable from the driver and passes it on to
    every Python worker, so all of them count into the same place and the driver
    serves the total (flink_jobs/metrics.py: serve_metrics).
    """
    directory = os.environ.setdefault(
        "PROMETHEUS_MULTIPROC_DIR",
        os.path.join(tempfile.gettempdir(), "distrosense-flink-metrics"),
    )
    os.makedirs(directory, exist_ok=True)
    for stale in glob.glob(os.path.join(directory, "*.db")):   # counters of a previous run
        try:
            os.remove(stale)
        except OSError:
            pass
    return directory


def connector_jars() -> list:
    jars = sorted(C.JARS_DIR.glob("*.jar"))
    if not jars:
        raise RuntimeError(
            f"No connector JARs in {C.JARS_DIR} — run `python flink_jobs/fetch_jars.py` first "
            "(the Docker image does this at build time).")
    return [jar.resolve().as_uri() for jar in jars]


def build_environment():
    from pyflink.common import Configuration, RestartStrategies
    from pyflink.datastream import CheckpointingMode, ExternalizedCheckpointCleanup, StreamExecutionEnvironment
    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend

    C.STATE_DIR.mkdir(parents=True, exist_ok=True)

    config = Configuration()
    # Python workers use the same interpreter (and therefore the same installed packages) as the driver.
    config.set_string("python.executable", sys.executable)
    config.set_string("python.client.executable", sys.executable)
    config.set_string("state.backend.rocksdb.localdir", C.ROCKSDB_DIR)
    config.set_string("taskmanager.memory.managed.size", "256m")      # RocksDB block cache + Python workers
    if C.WEB_UI_PORT:
        config.set_string("local.start-webserver", "true")
        config.set_string("rest.bind-address", "0.0.0.0")
        config.set_string("rest.port", str(C.WEB_UI_PORT))

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(C.PARALLELISM)
    env.add_jars(*connector_jars())

    # RocksDB state backend (incremental checkpoints) — the dedup state's home.
    env.set_state_backend(EmbeddedRocksDBStateBackend(True))

    # Checkpointing every 30s, exactly-once (aligned barriers); the Kafka sinks commit their
    # transactions when a checkpoint completes — the two-phase commit.
    env.enable_checkpointing(C.CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)
    checkpoints = env.get_checkpoint_config()
    checkpoints.set_checkpoint_storage_dir(C.CHECKPOINT_DIR)
    checkpoints.set_checkpoint_timeout(120_000)
    checkpoints.set_min_pause_between_checkpoints(5_000)
    checkpoints.set_tolerable_checkpoint_failure_number(3)
    checkpoints.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(C.RESTART_ATTEMPTS, C.RESTART_DELAY_MS))
    return env
