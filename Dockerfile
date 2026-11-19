FROM apache/airflow:3.3.1-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark==4.0.1" "PyYAML>=6,<7"
RUN url=https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-streaming/3.4.1/hadoop-streaming-3.4.1.jar \
    && curl -fsSL "$url" -o /opt/airflow/hadoop-streaming.jar \
    && echo "df576eca45de4b72c4b45985471668359533f2039124d514dfd9e07af012230d  /opt/airflow/hadoop-streaming.jar" | sha256sum -c -
COPY --chown=airflow:root src /opt/airflow/project/src
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root scripts /opt/airflow/project/scripts
ENV PYTHONPATH=/opt/airflow/project
