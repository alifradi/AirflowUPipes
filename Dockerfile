ARG AIRFLOW_VERSION=2.9.2
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY ./data_getters /opt/airflow/data_getters
COPY ./dags /opt/airflow/dags

# Create directories used by raw_getters.py and DAGs for outputs and logs
RUN mkdir -p /opt/airflow/master_database/init-scripts
RUN mkdir -p /opt/airflow/logs/

WORKDIR /opt/airflow