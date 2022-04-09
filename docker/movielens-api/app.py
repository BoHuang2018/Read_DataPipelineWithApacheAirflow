import logging
import time
import os
import pandas as pd

from api_services.offset_and_limit_of_rating_sample_class import OffsetAndLimitOfRatingSample
from api_services.start_end_date_of_rating_sample_class import StartEndDateOfRatingSample

from flask import Flask, request, jsonify, Response
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

from api_services.sorted_rating_sample_from_csv_class import SortedRatingsSampleFromCSV
from api_protocols import StartEndDateTimeStampYMD, OffsetAndLimitOfSamples
DEFAULT_ITEMS_PER_PAGE = 100


class RatingsApiController:
    """
    This is controller of API to retrieve samples of ratings. Its property methods will
    manage routes of API

    :attr: ratings_sample_generator: object implements protocol RatingsSampleGenerator,
                                     to generate sorted samples of ratings in format of pandas.DataFrame
    :attr: start_end_date_timestamp_YMD: object implements protocol StartEndDateTimeStampYMD,
                                         to manage start date and end date of retrieved samples of ratings
    :attr: offset_and_limit_of_samples: object implements protocol OffsetAndLimitOfSamples,
                                        to manage value of offset and limit amount of retrieved samples of ratings
    """
    def __init__(self,
                 generated_ratings_sample: pd.DataFrame,
                 start_end_date_timestamp_YMD: StartEndDateTimeStampYMD,
                 offset_and_limit_of_samples: OffsetAndLimitOfSamples):

        self.start_end_date_timestamp_YMD = start_end_date_timestamp_YMD
        self.offset_and_limit_of_samples = offset_and_limit_of_samples

        self.start_date_timestamp = 0
        self._get_start_date_timestamp_int()

        self.end_date_timestamp = 0
        self._get_end_date_timestamp_int()

        self.generated_ratings_sample = generated_ratings_sample
        self._tailor_rating_samples_by_start_date()
        self._tailer_rating_samples_by_end_date()

        self.offset_of_samples = 0
        self._get_offset_of_samples()

        self.limit_of_samples = 0
        self._get_limit_of_samples()

    def _get_start_date_timestamp_int(self):
        self.start_date_timestamp = self.start_end_date_timestamp_YMD.start_date_to_timestamp()

    def _get_end_date_timestamp_int(self):
        self.end_date_timestamp = self.start_end_date_timestamp_YMD.end_date_to_timestamp()

    def _get_offset_of_samples(self):
        self.offset_of_samples = self.offset_and_limit_of_samples.get_arg_offset_of_samples_value()

    def _get_limit_of_samples(self):
        self.limit_of_samples = self.offset_and_limit_of_samples.get_arg_limit_of_samples_value()

    def _tailor_rating_samples_by_start_date(self):
        if self.start_date_timestamp != 0:
            self.generated_ratings_sample = \
                self.generated_ratings_sample.loc[
                    self.generated_ratings_sample["timestamp"] >= self.start_date_timestamp
                    ]

    def _tailer_rating_samples_by_end_date(self):
        if self.end_date_timestamp != 0:
            self.generated_ratings_sample = \
                self.generated_ratings_sample.loc[
                    self.generated_ratings_sample["timestamp"] < self.end_date_timestamp
                    ]

    @property
    def ratings_samples(self) -> Response:
        try:
            samples_to_response = self.generated_ratings_sample.iloc[
                self.offset_of_samples + self.limit_of_samples]
        except Exception as e:
            logging.error("failed to extract ratings sample indexed by offset and limit")
            raise e
        return jsonify(
            {
                "result": samples_to_response.to_dict(orient="record"),
                "offset": self.offset_of_samples,
                "limit": self.limit_of_samples,
                "total": samples_to_response.shape[0],
            }
        )

# TODO: apply the class RatingsApiController to router docker/movielens-api/app.py:121


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

    start_date_timestamp = _date_to_timestamp(request.args.get('start_date', None))
    end_date_timestamp = _date_to_timestamp(request.args.get('end_date', None))

    # offset = int(request.args.get('offset', 0))
    # limit = int(request.args.get('limit', DEFAULT_ITEMS_PER_PAGE))

    start_end_date_of_rating_sample = StartEndDateOfRatingSample(

    )

    offset_and_limit_of_rating_sample = OffsetAndLimitOfRatingSample(
        arg_offset_of_samples_key='offset',
        arg_offset_of_samples_default_value=0,
        arg_limit_of_samples_key='limit',
        arg_limit_of_samples_default_value=DEFAULT_ITEMS_PER_PAGE
    )
    # TODO: to be continued
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
    app.run(host="0.0.0.0", port=5001)
