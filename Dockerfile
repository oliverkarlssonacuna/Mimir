FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
COPY src/ ./src/

RUN pip install --no-cache-dir \
    google-cloud-bigquery \
    google-genai \
    python-dotenv \
    requests

ENV PYTHONPATH=/app/src

COPY snapshot_job.py .

CMD ["python3", "snapshot_job.py"]
