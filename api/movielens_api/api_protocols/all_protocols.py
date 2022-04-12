import pandas as pd

from typing import Protocol


class RatingsSampleGenerator(Protocol):
    def generate_sorted_ratings_sample(self) -> pd.DataFrame:
        """A sorted ratings sample in format of pandas DataFrame is generated """


class StartEndDateTimeStampYMD(Protocol):
    def start_date_to_timestamp(self) -> int:
        """Get integer of start date in format of %Y-%m-%d """

    def end_date_to_timestamp(self) -> int:
        """Get integer of end date in format of %Y-%m-%d """


class OffsetAndLimitOfSamples(Protocol):
    def get_arg_offset_of_samples_value(self) -> int:
        """Get integer of offset of retrieved samples """

    def get_arg_limit_of_samples_value(self) -> int:
        """Get integer of limit (maximal) number of retrieved samples """
