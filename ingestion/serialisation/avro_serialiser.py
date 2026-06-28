import io
import json
import fastavro
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent / "schemas" / "span_batch.avsc"

with open(SCHEMA_PATH) as f:
    SCHEMA = fastavro.parse_schema(json.load(f))


def serialise(record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, SCHEMA, record)
    return buf.getvalue()


def deserialise(data: bytes) -> dict:
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, SCHEMA)