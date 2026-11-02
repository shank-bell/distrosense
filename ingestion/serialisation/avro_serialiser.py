import io
import json
import fastavro
from pathlib import Path
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

SCHEMA_PATH = Path(__file__).parent / "schemas" / "span_batch.avsc"

with open(SCHEMA_PATH) as f:
    _SCHEMA_DICT = json.load(f)
SCHEMA_STR = json.dumps(_SCHEMA_DICT)
SCHEMA = fastavro.parse_schema(_SCHEMA_DICT)


class SpanAvroSerializer:
    """
    Confluent wire format: 0x00 magic byte + 4-byte Schema Registry id + Avro
    body. The schema is registered under `<topic>-value` on first use, so
    consumers can decode with nothing but the registry URL.
    """

    def __init__(self, registry_url: str, topic: str):
        self._topic = topic
        client = SchemaRegistryClient({"url": registry_url})
        self._serializer = AvroSerializer(client, SCHEMA_STR, lambda record, ctx: record)

    def __call__(self, record: dict) -> bytes:
        return self._serializer(record, SerializationContext(self._topic, MessageField.VALUE))


# Schemaless helpers (no registry framing) — kept for tests/tools.
def serialise(record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, SCHEMA, record)
    return buf.getvalue()


def deserialise(data: bytes) -> dict:
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, SCHEMA)
