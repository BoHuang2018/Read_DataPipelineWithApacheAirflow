import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

dag_bash = DAG(
    dag_id="chapter04_stock_sense_bash_operator",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
)

get_data = BashOperator(
    task_id="get_data",
    bash_command=(  # for example: https://dump.wikimedia.org/other/pageviews/2019/2019-07/pageviews-20190701-010000.gz
        "curl -o /tmp/wikipageviews.gz "
        "https://dump.wikimedia.org/other/pageviews/{{ execution_date.year }}/"  # like: .../other/pageviews/2019/              
        "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"  # like:  /2019-07/
        "pageviews-{{ execution_date.year }}{{ '{:02}'.format(execution_date.month) }}"  # like: pageviews-201907
        "{{ '{:02}'.format(execution_date.day) }}-{{ '{:02}'.format(execution_date.hour) }}0000.gz"  # like: 01-010000.gz
    ),
    dag=dag_bash,
)

