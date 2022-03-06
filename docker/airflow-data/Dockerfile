ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.2.4-python3.9"
FROM ${AIRFLOW_BASE_IMAGE}
USER root
RUN mkdir -p /data && chown airflow /data
USER airflow