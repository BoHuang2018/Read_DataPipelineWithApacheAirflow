import logging
import pandas as pd

from dataclasses import dataclass


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
            logging.error(
                f"failed to read ratings from csv file in path: f{self.csv_file_path} by unknown error {e=}")
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
