import logging
import time
from dataclasses import dataclass

import pandas as pd
from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

DEFAULT_ITEMS_PER_PAGE = 100


def _date_to_timestamp(date_str) -> int:
    """
    :param date_str: timestamp of date, in string, in format "%Y-%m-%d"
    :return integer of the date_str
    """
    if date_str is None:
        raise ValueError("argument data_str cannot be None")
    try:
        date_int = int(time.mktime(time.strptime(date_str, "%Y-%m-%d")))
    except Exception as e:
        logging.error("failed to get integer of date in format of %Y-%m-%d")
        raise e
    return date_int


@dataclass(frozen=True)
class SortedRatingsSampleFromCSV:
    """
    This class is to generate a sorted dataframe as sample through the following steps
    1. Read ratings from a csv file and generate pandas DataFrame
    2. Generate sorted sample of ratings in format of pandas DataFrame

    :attr: csv_file_path: path of csv file containing ratings
    :attr: number_of_samples: number of samples will be generated
    :attr: random_state: random seed to reproduce random sample
    :attr: sort_values: which columns the sorting will be implemented by
    """
    csv_file_path: str
    number_of_samples: int
    random_state: int
    sort_values: list

    def _read_ratings_from_csv(self) -> pd.DataFrame:
        """
        Step 1: Read ratings from a csv file and generate pandas DataFrame
        """
        try:
            ratings_from_csv = pd.read_csv(self.csv_file_path)
        except Exception as e:
            logging.error(f"failed to read ratings from csv file in path: f{self.csv_file_path}")
            raise e
        return ratings_from_csv

    @property
    def generate_sorted_ratings_sample(self) -> pd.DataFrame:
        """
        Generate sorted sample of ratings in format of pandas DataFrame
        """
        ratings_from_csv = self._read_ratings_from_csv()
        ratings_sample = ratings_from_csv.sample(n=self.number_of_samples, random_state=self.random_state)
        sorted_ratings_sample = ratings_sample.sort_values(by=self.sort_values)
        return sorted_ratings_sample


app = Flask(__name__)

rating_sample_generator = SortedRatingsSampleFromCSV(
    csv_file_path="/ratings.csv", number_of_samples=100000, random_state=0,
    sort_values=["timestamp", "userId", "movieId"])

app.config["rating"] = rating_sample_generator.generate_sorted_ratings_sample

auth = HTTPBasicAuth()
users = {os.environ["API_USER"]: generate_password_hash(os.environ["API_PASSWORD"])}


@auth.verify_password
def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    return False


@app.route("/")
def hello():
    return "Hello from the Movie Rating API"


@app.route("/ratings")
@auth.login_required
def ratings():
    """
    Get ratings from the movielens dataset

    Parameters
    ----------
    start_date: str
        Start date to query from (inclusive)
    end_date: str
        End date to query upto (exclusive)
    offset: int
        Offset to start returning data from (used for pagination)
    limit:
        Maximum number of records to return (used for pagination)

    :return: directory
    """

    start_date_timestamp = _date_to_timestamp(request.args.get('start_date', None))
    end_date_timestamp = _date_to_timestamp(request.args.get('end_date', None))

    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', DEFAULT_ITEMS_PER_PAGE))

    ratings_df = app.config.get("ratings")

    if start_date_timestamp:
        ratings_df = ratings_df.loc[ratings_df["timestamp"] >= start_date_timestamp]

    if end_date_timestamp:
        rating_df = ratings_df.loc[ratings_df["timestamp"] < end_date_timestamp]

    subset = rating_df.iloc[offset: offset + limit]

    return jsonify(
        {
            "result": subset.to_dict(orient="record"),
            "offset": offset,
            "limit": limit,
            "total": ratings_df.shape[0],
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
