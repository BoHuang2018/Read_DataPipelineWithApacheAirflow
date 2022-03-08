from urllib import request

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

dag_bash = DAG(
    dag_id="stock_sense_python_operator_take_params",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
)


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dump.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ executor_date.year }}",
        "month": "{{ executor_date.month }}",
        "day": "{{ executor_date.day }}",
        "hour": "{{ executor_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag,
)