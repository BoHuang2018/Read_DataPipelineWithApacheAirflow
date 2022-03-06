from pathlib import Path

import pandas
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="scheduled",
    start_date=pendulum.DateTime(2019, 1, 1),
    end_date=pendulum.DateTime(2019, 1, 5),
    schedule_interval="@daily",
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events.json "
        "http://events_api:5000/events?"
        "start_date={{ds)}}&"
        "end_date={{next_ds}}"
    ),
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    """Calculate event statistics"""
    Path(output_path).parent.mkdir(exist_ok=True)
    events = pandas.read_json(input_path)
    stats = events.groupby(["date", "users"]).size().reset_index()
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats
