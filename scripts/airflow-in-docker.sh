#!/bin/bash
set -x

SCRIPT_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

docker run \
-ti \
-p 8080:8080 \
-v ${SCRIPT_DIR}/../dags/download_and_process_rocket_launch_data.py:/opt/airflow/dags/download_and_process_rocket_launch_data.py \
--name airflow
--entrypoint=/bin/bash \
apache/airflow:2.0.0-python3.9 \
-c '( \
airflow db init && \
airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.org \
); \
airflow webserver & \
airflow scheduler \
'
