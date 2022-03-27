import time

import pandas as pd
from flask import Flask, request, jsonify
from requests.auth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

DEFAULT_ITEMS_PER_PAGE = 100


def _read_ratings(file_path):
    ratings = pd.read_csv(file_path)
    ratings = ratings.sample(n=100000, random_state=0)  # subsample dataset
    ratings = ratings.sort_values(by=["timestamp", "userId", "movieId"])  # sorting for convenience
    return ratings


app = Flask(__name__)
app.config["rating"] = _read_ratings("/ratings.csv")

auth = HTTPBasicAuth()
users = {os.environ["API_USER"]: generate_password_hash(os.environ["API_PASSWORD"])}


def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    return False


def hello():
    return "Hello from the Movie Rating API"


def _date_to_timestamp(date_str):
    if date_str is None:
        return None
    return int(time.mktime(time.strptime(date_str, "%Y-%m-%d")))


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
