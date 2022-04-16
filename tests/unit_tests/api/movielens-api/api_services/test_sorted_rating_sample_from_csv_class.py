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