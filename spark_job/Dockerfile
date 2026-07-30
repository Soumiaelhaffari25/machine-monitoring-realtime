FROM apache/spark:3.5.1-python3

USER root

WORKDIR /app

RUN pip3 install --no-cache-dir pymongo==4.8.0 numpy==1.24.4 prometheus-client==0.20.0

COPY spark_job/streaming_job.py .
COPY spark_job/rules.py .
COPY spark_job/mongo_sink.py .
COPY producer/schemas ./schemas

EXPOSE 8002

CMD ["python3", "streaming_job.py"]