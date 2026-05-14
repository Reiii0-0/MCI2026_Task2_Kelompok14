FROM apache/airflow:2.9.1-python3.11
USER root

# Install Java untuk kebutuhan Apache Spark
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
