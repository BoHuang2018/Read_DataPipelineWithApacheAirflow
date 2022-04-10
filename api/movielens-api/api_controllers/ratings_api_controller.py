import os.path
import sys
import logging
import pandas as pd
from flask import jsonify, Response

script_dir = os.path.dirname(__file__)
api_protocols_dir = os.path.join(script_dir, '..', 'api_protocols')
sys.path.append(api_protocols_dir)

from all_protocols import StartEndDateTimeStampYMD, OffsetAndLimitOfSamples


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
        self.start_date_timestamp = self.start_end_date_timestamp_YMD.start_date_to_timestamp
        logging.info(f"got self.start_date_timestamp: {self.start_date_timestamp}")

    def _get_end_date_timestamp_int(self):
        self.end_date_timestamp = self.start_end_date_timestamp_YMD.end_date_to_timestamp

    def _get_offset_of_samples(self):
        self.offset_of_samples = self.offset_and_limit_of_samples.get_arg_offset_of_samples_value

    def _get_limit_of_samples(self):
        self.limit_of_samples = self.offset_and_limit_of_samples.get_arg_limit_of_samples_value

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
            logging.info(f"type(samples_to_response) == {samples_to_response}")
        except Exception as e:
            logging.error("failed to extract ratings sample indexed by offset and limit")
            raise e

        try:
            response = jsonify(
                {
                    "result": samples_to_response.to_dict(orient="record"),
                    "offset": self.offset_of_samples,
                    "limit": self.limit_of_samples,
                    "total": samples_to_response.shape[0],
                }
            )
        except TypeError as e:
            raise TypeError(
                f"failed to generate response by rating samples which in type of {type(samples_to_response)}"
            )
        return response
