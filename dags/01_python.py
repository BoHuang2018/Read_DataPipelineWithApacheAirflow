import json
import logging
import os
import datetime as dt

import pandas as pd
import requests
from airflow import DAG

# Set default values for environmental variables named as "MOVIELENS_HOST", "MOVIELENS_SCHEMA" and "MOVIELENS_PORT"
from airflow.operators.python import PythonOperator

MOVIELENS_HOST = os.environ.get("MOVIELENS_HOST", "movielens")
MOVIELENS_SCHEMA = os.environ.get("MOVIELENS_SCHEMA", "http")
MOVIELENS_PORT = os.environ.get("MOVIELENS_PORT", "5000")

# Get values of environmental variables named as "MOVIELENS_USER" and "MOVIELENS_PASSWORD", set in docker-compose.yaml
MOVIELENS_USER = os.environ["MOVIELENS_USER"]
MOVIELENS_PASSWORD = os.environ["MOVIELENS_PASSWORD"]


def _get_session():
    """Builds a requests session for the Movielens API"""
    # Setup our requests session
    session = requests.Session()
    session.auth = (MOVIELENS_USER, MOVIELENS_PASSWORD)
    
    # Define API base url from connection details
    schema = MOVIELENS_SCHEMA
    host = MOVIELENS_HOST
    port = MOVIELENS_PORT
    
    base_url = f"{schema}://{host}:{port}"
    
    return session, base_url


def _get_with_pagination(session, url, params, batch_size=100):
    """
    Fetches records using a get request with given url/params, taking pagination into account
    :param session:
    :param params:
    :param batch_size:
    :return:
    """
    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(
            url, params={**params, **{"offset": offset, "limit": batch_size}}
        )
        response.raise_for_status()
        response_json = response.json()

        yield from response_json["result"]
        offset += batch_size
        total = response_json["total"]


def _get_ratings(start_date, end_date, batch_size=100):
    session, base_url = _get_session()

    yield from _get_with_pagination(
        session=session,
        params={"start_date": start_date, "end_date": end_date},
        batch_size=batch_size,
    )


with DAG(
    dag_id="01_python",
    description="Fetches ratings from the Movielens API using the Python Operator",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 10),
    schedule_interval="@daily",
) as dag:

    def _fetch_ratings(templates_dict, batch_size=1000, **_):
        logger = logging.getLogger(__name__)
        start_date = templates_dict["start_date"]
        end_date = templates_dict["end_date"]
        output_path = templates_dict["output_path"]

        logger.info(f"Fetching ratings for {start_date} to {end_date}")
        ratings = list(
            _get_ratings(
                start_date=start_date, end_date=end_date, batch_size=batch_size
            )
        )
        logger.info(f"Fetched {len(ratings)} ratings")
        logger.info(f"Writing ratings to {output_path}")

        # Make sure output directory exists
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(output_path, "w") as file_:
            json.dump(ratings, fp=file_)

    fetch_ratings = PythonOperator(
        task_id="fetch_ratings",
        python_callable=_fetch_ratings,
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/data/python/ratings/{{ds}}.json",
        },
    )

    def _rank_movies(templates_dict, min_ratings=2, **_):
        # TODO: read progression resume from here