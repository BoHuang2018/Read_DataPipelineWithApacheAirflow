import os

from api_services.offset_and_limit_of_rating_sample_class import OffsetAndLimitOfRatingSample
from api_services.start_end_date_of_rating_sample_class import StartEndDateOfRatingSample

from flask import Flask
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

from api_services.sorted_rating_sample_from_csv_class import SortedRatingsSampleFromCSV
from api_controllers.ratings_api_controller import RatingsApiController

DEFAULT_ITEMS_PER_PAGE = 100

app = Flask(__name__)

rating_sample_generator = SortedRatingsSampleFromCSV(
    csv_file_path="ratings.csv", number_of_samples=100000, random_state=0,
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

    start_end_date_of_rating_sample = StartEndDateOfRatingSample(
        arg_start_date_key='start_date',
        arg_start_date_default_value='2019-01-01',
        arg_end_date_key='end_date',
        arg_end_date_default_value='2019-01-02'
    )

    offset_and_limit_of_rating_sample = OffsetAndLimitOfRatingSample(
        arg_offset_of_samples_key='offset',
        arg_offset_of_samples_default_value=0,
        arg_limit_of_samples_key='limit',
        arg_limit_of_samples_default_value=DEFAULT_ITEMS_PER_PAGE
    )

    ratings_api_controller = RatingsApiController(
        generated_ratings_sample=app.config.get("ratings"),
        start_end_date_timestamp_YMD=start_end_date_of_rating_sample,
        offset_and_limit_of_samples=offset_and_limit_of_rating_sample
    )

    rating_samples = ratings_api_controller.ratings_samples

    return rating_samples


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
