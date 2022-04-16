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
        self.assertTrue(ratings_from_test_csv.equals(self.sorted_rating_sample_from_csv._read_ratings_from_csv()))
