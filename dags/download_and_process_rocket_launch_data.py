import json
import pathlib
import requests
import requests.exceptions as requests_exceptions
import airflow.utils.dates as airflow_utils_dates
import airflow.DAG as DAG
import airflow.operators.bash.BashOperator as BashOperator
import airflow.operators.bash.PythonOperator as PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow_utils_dates.day_ago(14),
    # instance of DAG will not run automatically, can be triggered from the Airflow UI
    schedule_interval=None,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)


def _get_pictures():
    # Ensure directory exists: Create pictures directory if it does not exist.
    pathlib.Path("tmp/images").mkdir(parents=True, exist_ok=True)

    # Open the result from the previous task
    with open("tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                # Download each image
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                # Store each image
                with open(target_file, "wb") as f:
                    f.write(response.content)
                # Print to stdout; this will be captured in Airflow logs
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images"',
    dag=dag,
)

# use binary right shift operator, i.e. `>>` to define dependencies between tasks√
download_launches >> get_pictures >> notify
