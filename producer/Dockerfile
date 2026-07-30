FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir confluent-kafka[avro]==2.5.3 requests==2.32.3 fastavro==1.9.7 numpy==1.26.4 prometheus-client==0.20.0

COPY producer/producer.py .
COPY producer/calibration.json .
COPY producer/schemas ./schemas

EXPOSE 8001

CMD ["python", "producer.py"]