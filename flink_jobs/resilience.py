import io
import json
import struct

import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient

from flink_jobs.config import SCHEMA_REGISTRY_URL


def safe_json_loads(raw: bytes):
    """
    Kafka value deserializer that returns None for an undecodable payload
    instead of raising — an exception inside the deserializer kills the whole
    consumer loop. Callers skip None.
    """
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None


class ThrottledErrorLog:
    """
    Logs the 1st, 101st, 201st... occurrence of an error. A per-message
    failure (DB down, bad record) at 100+ msg/s must not flood the log.
    """

    def __init__(self, name: str, every: int = 100):
        self._name = name
        self._every = every
        self.count = 0

    def __call__(self, exc: Exception):
        self.count += 1
        if self.count % self._every == 1:
            print(f"[{self._name}] message processing failed (#{self.count}): "
                  f"{type(exc).__name__}: {exc}")


class SpanDecoder:
    """
    raw-telemetry now carries two encodings, and this decodes both to a dict
    MetricEvent.from_dict understands:
      - Avro in the Confluent wire format (0x00 magic byte + 4-byte Schema
        Registry id + Avro body) — what the ingestion service writes. The
        writer schema is fetched from the registry by id and cached.
      - JSON — what the simulator writes in SIM_PUBLISH_MODE=kafka, and what
        is still sitting in the topic from before the ingestion rewiring.
    Anything undecodable (or a registry outage) becomes None, logged but
    never raised, so one bad message can't kill the consumer loop.
    """

    def __init__(self, registry_url: str, on_failure=None):
        self._registry = SchemaRegistryClient({"url": registry_url})
        self._schemas: dict = {}
        self._log = ThrottledErrorLog("SpanDecoder", every=1000)
        self._on_failure = on_failure

    def __call__(self, raw: bytes):
        if not raw:
            return None
        if raw[0] == 0 and len(raw) > 5:
            return self._decode_avro(raw)
        return safe_json_loads(raw)

    def _decode_avro(self, raw: bytes):
        try:
            schema_id = struct.unpack(">I", raw[1:5])[0]
            schema = self._schemas.get(schema_id)
            if schema is None:
                schema = fastavro.parse_schema(json.loads(self._registry.get_schema(schema_id).schema_str))
                self._schemas[schema_id] = schema
            return fastavro.schemaless_reader(io.BytesIO(raw[5:]), schema)
        except Exception as e:
            self._log(e)
            if self._on_failure:
                self._on_failure()
            return None


def _count_decode_failure():
    from flink_jobs import metrics as M
    M.DECODE_FAILURES.inc()


decode_span = SpanDecoder(SCHEMA_REGISTRY_URL, on_failure=_count_decode_failure)
