"""
Download the Flink connector JARs that PyFlink does not bundle.

PyFlink ships the Flink runtime but not the Kafka connector, and PyFlink's
DataStream API cannot resolve Maven dependencies, so the JAR has to sit on disk
and be passed to the job with env.add_jars(). Each JAR is pinned by version and
SHA-1 (from Maven Central) so a corrupted or swapped download is rejected.

    python flink_jobs/fetch_jars.py        # writes into flink_jobs/lib/
"""
import hashlib
import os
import sys
import urllib.request

LIB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib")

MAVEN = "https://repo1.maven.org/maven2"
JARS = [
    # (group path, artifact, version, sha1)
    ("org/apache/flink", "flink-sql-connector-kafka", "3.0.2-1.18",
     "2c2d486dfc3fb759ee3b8c9dc5e1f21dd9b74d2d"),
]


def _sha1(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_all() -> list:
    os.makedirs(LIB_DIR, exist_ok=True)
    paths = []
    for group, artifact, version, sha1 in JARS:
        name = f"{artifact}-{version}.jar"
        dest = os.path.join(LIB_DIR, name)
        if not (os.path.exists(dest) and _sha1(dest) == sha1):
            url = f"{MAVEN}/{group}/{artifact}/{version}/{name}"
            print(f"[fetch_jars] downloading {url}")
            tmp = dest + ".part"
            urllib.request.urlretrieve(url, tmp)
            got = _sha1(tmp)
            if got != sha1:
                os.remove(tmp)
                sys.exit(f"[fetch_jars] SHA-1 mismatch for {name}: expected {sha1}, got {got}")
            os.replace(tmp, dest)
        print(f"[fetch_jars] ok  {name}  ({os.path.getsize(dest):,} bytes)")
        paths.append(dest)
    return paths


if __name__ == "__main__":
    fetch_all()
