FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka/    ./kafka/
COPY dataflow/ ./dataflow/
COPY airflow/  ./airflow/

ENV PYTHONUNBUFFERED=1
CMD ["python", "kafka/kafka_to_pubsub.py"]
