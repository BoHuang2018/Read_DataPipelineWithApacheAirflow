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
        Step 1. Read ratings from a csv file and generate pandas DataFrame
        """
        try:
            ratings_from_csv = pd.read_csv(self.csv_file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"failed to read csv file because of problem of path: {self.csv_file_path}")
        except Exception as e:
            logging.error(
                f"failed to read ratings from csv file in path: {self.csv_file_path} by unexpected error {e=}")
            raise e
        return ratings_from_csv

    def _check_sort_values_list(self):
        if len(self.sort_values) == 0:
            raise ValueError("the attribute, sort_values, cannot be an empty list")
        for v in self.sort_values:
            if not isinstance(v, str):
                raise ValueError(f"the attribute, sort_values, must be list of strings, "
                                 f"but the element: {v} is in type of {type(v).__name__}")
        return

    @property
    def generate_sorted_ratings_sample(self) -> pd.DataFrame:
        """
        Step 2. Generate sorted sample of ratings in format of pandas DataFrame
        """
        self._check_sort_values_list()
        ratings_from_csv = self._read_ratings_from_csv()
        try:
            ratings_sample = ratings_from_csv.sample(n=self.number_of_samples, random_state=self.random_state)
            sorted_ratings_sample = ratings_sample.sort_values(by=self.sort_values)
        except KeyError:
            raise KeyError(
                f"failed to generate sorted rating sample because attribute, sort_values: {self.sort_values}, "
                f"involve element(s) cannot be found in Dataframe's columns"
            )
        except Exception as e:
            logging.error(
                f"failed to generate sorted rating sample from csv file in path: {self.csv_file_path} "
                f"by unexpected error {e=}"
            )
            raise e
        return sorted_ratings_sample
