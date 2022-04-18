from dataclasses import dataclass

import pandas as pd
from unittest import TestCase

from api.movielens_api.api_services.sorted_rating_sample_from_csv_class import SortedRatingsSampleFromCSV


class TestSortedRatingsSampleFromCSV(TestCase):
    def setUp(self):
        csv_file_path = "../ratings_copy_for_test.csv"
        number_of_samples = 100
        random_state = 0
        sort_values = ["timestamp", "userId", "movieId"]

        self.sorted_rating_sample_from_csv = SortedRatingsSampleFromCSV(
            csv_file_path=csv_file_path,
            number_of_samples=number_of_samples,
            random_state=random_state,
            sort_values=sort_values
        )


class TestSortedRatingsSampleFromCSVWithProperAttributes(TestSortedRatingsSampleFromCSV):
    def test_read_ratings_from_csv(self):
        """
        Test the method '_read_ratings_from_csv', with path to data file 'ratings_copy_for_test.csv'
        """
        ratings_from_test_csv = pd.read_csv("../ratings_copy_for_test.csv")
        print(ratings_from_test_csv.columns)
        self.assertTrue(ratings_from_test_csv.equals(self.sorted_rating_sample_from_csv._read_ratings_from_csv()))

    def test_generate_sorted_ratings_sample(self):
        """
        Test the method 'generate_sorted_ratings_sample', with test csv file 'ratings_copy_for_test.csv',
        and other proper attributes:
            number_of_samples = 100
            random_state = 0
            sort_values = ["timestamp", "userId", "movieId"]
        """
        sorted_ratings_sample_1 = self.sorted_rating_sample_from_csv.generate_sorted_ratings_sample
        sorted_ratings_sample_2 = self.sorted_rating_sample_from_csv.generate_sorted_ratings_sample
        # Both two samples are in type of Dataframe
        self.assertTrue(isinstance(sorted_ratings_sample_1, pd.DataFrame))
        self.assertTrue(isinstance(sorted_ratings_sample_2, pd.DataFrame))
        # Each of samples has 100 rows
        self.assertEqual(sorted_ratings_sample_1.shape[0], 100)
        self.assertEqual(sorted_ratings_sample_2.shape[0], 100)
        # The two sample are identical because of a common random_state and columns for sorting
        self.assertTrue(sorted_ratings_sample_1.equals(sorted_ratings_sample_2))


class TestSortedRatingsSampleFromCSVWrongAttributes(TestCase):
    """
    Initialize an object of class SortedRatingsSampleFromCSV with wrong attributes:
    1. csv_file_path_wrong:
        1.1. a path with a folder which does not exist
        1.2. path is empty string: ""
        1.3. path is black space in string: " "
    2. number_of_samples_wrong:
        2.1. bigger that number of rows in the csv file (1200634 rows). It is set as 1300000.
        2.2. negative integer, like -1
    3. random_state_wrong: Not in type of integer
    4. sort_values_wrong:
        4.1. Empty list
        4.2. At least one element are not in type of string
        4.3. At least one element are not found in the related dataframe's columns ( userId, movieId, rating, timestamp)
    """

    def test_read_ratings_from_csv_in_wrong_path(self):
        """
        Initialize an object of class SortedRatingsSampleFromCSV with wrong attributes:
        1. csv_file_path_wrong:
            1.1. a path with a folder which does not exist
            1.2. path is empty string: ""
            1.3. path is black space in string: " "
        """
        @dataclass
        class TestCaseWithWrongAttrs:
            csv_file_path: str
            number_of_samples: int
            random_state: int
            sort_values: list

        test_cases_with_wrong_path_to_csv_file = [
            TestCaseWithWrongAttrs(
                csv_file_path="../not_existed_folder/ratings_copy_for_test.csv",
                number_of_samples=100,
                random_state=0,
                sort_values=["timestamp", "userId", "movieId"]
            ),
            TestCaseWithWrongAttrs(
                csv_file_path="",
                number_of_samples=100,
                random_state=0,
                sort_values=["timestamp", "userId", "movieId"]
            ),
            TestCaseWithWrongAttrs(
                csv_file_path=" ",
                number_of_samples=100,
                random_state=0,
                sort_values=["timestamp", "userId", "movieId"]
            )
        ]
        for tc in test_cases_with_wrong_path_to_csv_file:
            self.sorted_rating_sample_from_csv = SortedRatingsSampleFromCSV(
                csv_file_path=tc.csv_file_path,
                number_of_samples=tc.number_of_samples,
                random_state=tc.random_state,
                sort_values=tc.sort_values
            )
            with self.assertRaises(Exception) as context:
                self.sorted_rating_sample_from_csv._read_ratings_from_csv()
            self.assertEqual(
                str(context.exception), f"failed to read csv file because of problem of path: {tc.csv_file_path}"
            )

    def test_generate_sorted_ratings_sample_with_wrong_sort_values(self):
        @dataclass
        class TestCaseWithWrongAttrs:
            csv_file_path: str
            number_of_samples: int
            random_state: int
            sort_values: list

        test_cases_with_wrong_sort_values = [
            TestCaseWithWrongAttrs(
                csv_file_path="../ratings_copy_for_test.csv",
                number_of_samples=100,
                random_state=0,
                sort_values=[]
            ),
            TestCaseWithWrongAttrs(
                csv_file_path="../ratings_copy_for_test.csv",
                number_of_samples=100,
                random_state=0,
                sort_values=["timestamp", "userId", 123]
            ),
            TestCaseWithWrongAttrs(
                csv_file_path="../ratings_copy_for_test.csv",
                number_of_samples=100,
                random_state=0,
                sort_values=["timestamp", "userId", "movieId", "not_existing_column"]
            )
        ]
        exception_types = ['ValueError', 'ValueError', 'KeyError']
        exception_messages = [
            "the attribute, sort_values, cannot be an empty list",
            "the attribute, sort_values, must be list of strings, but the element: 123 is in type of int",
            "\"failed to generate sorted rating sample because attribute, "
            "sort_values: ['timestamp', 'userId', 'movieId', 'not_existing_column'], "
            "involve element(s) cannot be found in Dataframe's columns\""
        ]

        for i in range(len(test_cases_with_wrong_sort_values)):
            tc = test_cases_with_wrong_sort_values[i]
            self.sorted_rating_sample_from_csv = SortedRatingsSampleFromCSV(
                csv_file_path=tc.csv_file_path,
                number_of_samples=tc.number_of_samples,
                random_state=tc.random_state,
                sort_values=tc.sort_values
            )
            with self.assertRaises(Exception) as context:
                _ = self.sorted_rating_sample_from_csv.generate_sorted_ratings_sample
            self.assertEqual(type(context.exception).__name__, exception_types[i])
            self.assertEqual(str(context.exception), exception_messages[i])

    def test_generate_sorted_ratings_sample_with_wrong_number_of_samples(self):
        """
        Initialize an object of class SortedRatingsSampleFromCSV with wrong attributes:
        2. number_of_samples_wrong:
            2.1. bigger that number of rows in the csv file (1200634 rows). It is set as 1300000.
            2.2. negative integer, like -1
        """
        @dataclass
        class TestCaseWithWrongAttrs:
            csv_file_path: str
            number_of_samples: int
            random_state: int
            sort_values: list

        test_cases_with_wrong_number_of_samples = [
            TestCaseWithWrongAttrs(
                csv_file_path="../ratings_copy_for_test.csv",
                number_of_samples=1300000,
                random_state=0,
                sort_values=["timestamp", "userId", "movieId"]
            ),
            TestCaseWithWrongAttrs(
                csv_file_path="../ratings_copy_for_test.csv",
                number_of_samples=-1,
                random_state=0,
                sort_values=["timestamp", "userId", "movieId"]
            ),
        ]
        for tc in test_cases_with_wrong_number_of_samples:

