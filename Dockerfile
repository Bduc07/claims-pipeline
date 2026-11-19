FROM apache/airflow:3.3.1-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark==4.0.1" "PyYAML>=6,<7"
COPY --chown=airflow:root src /opt/airflow/project/src
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow/project
