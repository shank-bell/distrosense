# PyFlink job image. The build context is the REPO ROOT (the job imports common/
# and observability/ as well as flink_jobs/):
#
#   docker build -f flink_jobs/Dockerfile -t distrosense-flink .
#
# (docker compose does this for you: docker compose --profile flink up -d --build flink)
# PyFlink starts a JVM for the Flink runtime (Flink 1.18 supports Java 8/11/17).
# Java 11 comes from the official Temurin image rather than apt: Debian's JDK 11 is
# not packaged for current releases, and this keeps the JRE version exact.
FROM eclipse-temurin:11-jre-jammy AS jre

FROM python:3.10-slim-bookworm
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Cap the JVM: it hosts the Flink runtime in-process. Python workers are separate processes.
ENV JAVA_TOOL_OPTIONS="-Xmx1g -XX:+UseG1GC"

WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1

COPY flink_jobs/requirements.txt flink_jobs/requirements.txt
RUN pip install --no-cache-dir -r flink_jobs/requirements.txt

# Kafka connector JAR (pinned + SHA-1 verified), cached in its own layer.
COPY flink_jobs/fetch_jars.py flink_jobs/fetch_jars.py
RUN python flink_jobs/fetch_jars.py

COPY flink_jobs flink_jobs
COPY common common
COPY observability observability

CMD ["python", "-m", "flink_jobs.jobs.anomaly_detection_job"]
