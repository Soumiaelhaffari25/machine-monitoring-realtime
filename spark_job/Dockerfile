FROM apache/spark:3.5.1-python3

USER root

WORKDIR /app

RUN pip3 install --no-cache-dir pymongo==4.8.0 numpy==1.24.4 prometheus-client==0.20.0

RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.1/spark-avro_2.12-3.5.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar


COPY spark_job/streaming_job.py .
COPY spark_job/rules.py .
COPY spark_job/mongo_sink.py .
COPY producer/schemas ./schemas

EXPOSE 8002


CMD ["/opt/spark/bin/spark-submit", "streaming_job.py"]