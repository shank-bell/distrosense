FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-c", "from flink_jobs.jobs.anomaly_detection_job import AnomalyDetectionJob; AnomalyDetectionJob().run()"]