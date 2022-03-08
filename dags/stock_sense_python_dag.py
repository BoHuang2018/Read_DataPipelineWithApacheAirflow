from urllib import request

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

dag_python = DAG(
    dag_id="stock_sense_python_operator",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
)


def _get_data(execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dump.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/tmp/wikipageviews.gz"
    request.urlretrieve(url, output_path)


get_data_python = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    dag=dag_python,
)


def _print_content(**context):
    print(context)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_content,
    dag=dag_python,
)


def _print_start_end_date(**context):
    start = context["execution_date"]
    end = context["next_execution_date"]
    print(f"Start: {start}, end: {end}")


print_start_end_date = PythonOperator(
    task_id="print_start_end_date",
    python_callable=_print_start_end_date,
    dag=dag_python,
)
