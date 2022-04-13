from unittest import TestCase

import flask
from api.movielens_api.api_services.offset_and_limit_of_rating_sample_class import OffsetAndLimitOfRatingSample


class TestOffsetAndLimitOfRatingSample(TestCase):
    def setUp(self):
        arg_offset_of_samples_key_test = "offset"
        arg_offset_of_samples_default_value = 0
        arg_limit_of_samples_key = "limit"
        arg_limit_of_samples_default_value = 100

        self.offset_and_limit_of_rating_sample = OffsetAndLimitOfRatingSample(
                arg_offset_of_samples_key=arg_offset_of_samples_key_test,
                arg_offset_of_samples_default_value=arg_offset_of_samples_default_value,
                arg_limit_of_samples_key=arg_limit_of_samples_key,
                arg_limit_of_samples_default_value=arg_limit_of_samples_default_value
            )


app = flask.Flask(__name__)


class TestOffsetAndLimitOfRatingSampleWithProperAttributes(TestOffsetAndLimitOfRatingSample):
    def test_get_arg_offset_of_samples_value(self):
        """
        Test the method with given argument 'offset'
        """
        with app.test_request_context('/ratings?offset=100'):
            self.assertEqual(100, self.offset_and_limit_of_rating_sample.get_arg_offset_of_samples_value)
        with app.test_request_context('/ratings?offset=100&limit=1000'):
            self.assertEqual(100, self.offset_and_limit_of_rating_sample.get_arg_offset_of_samples_value)
        """
        Test the method without given argument 'offset', it should return default value, 0
        """
        with app.test_request_context('/ratings'):
            self.assertEqual(0, self.offset_and_limit_of_rating_sample.get_arg_offset_of_samples_value)
        with app.test_request_context('/ratings?limit=1000'):
            self.assertEqual(0, self.offset_and_limit_of_rating_sample.get_arg_offset_of_samples_value)

    def test_get_arg_limit_of_samples_value_with_given_argument(self):
        """
        Test the method with given argument 'limit'
        """
        with app.test_request_context('/ratings?limit=101'):
            self.assertEqual(101, self.offset_and_limit_of_rating_sample.get_arg_limit_of_samples_value)
        with app.test_request_context('/ratings?limit=101&offset=100'):
            self.assertEqual(101, self.offset_and_limit_of_rating_sample.get_arg_limit_of_samples_value)
        """
        Test the method without given argument 'offset', it should return default value, 100
        """
        with app.test_request_context('/ratings'):
            self.assertEqual(100, self.offset_and_limit_of_rating_sample.get_arg_limit_of_samples_value)
        with app.test_request_context('/ratings?offset=100'):
            self.assertEqual(100, self.offset_and_limit_of_rating_sample.get_arg_limit_of_samples_value)


class TestOffsetAndLimitOfRatingSampleWithWrongAttributes(TestOffsetAndLimitOfRatingSample):
    """
    Test the two @property methods will raise error when
    1. value of argument, offset, cannot be converted to integer from string
    2. value of argument, limit, cannot be converted to integer from string
    """
    no_integer_values = ['1.1', '1.0', 'abc', 'True', 'False', '@#$123abc', '...']

    def test_get_arg_offset_of_samples_value_with_value_cannot_be_integer(self):
        for v in self.no_integer_values:
            with self.assertRaises(Exception) as exception_context:
                with app.test_request_context(f'/ratings?offset={v}'):
                    self.assertEqual(100, self.offset_and_limit_of_rating_sample.get_arg_offset_of_samples_value)
            self.assertRaises(Exception, exception_context)

    def test_get_arg_limit_of_samples_value_with_value_cannot_be_integer(self):
        for v in self.no_integer_values:
            with self.assertRaises(Exception) as exception_context:
                with app.test_request_context(f'/ratings?limit={v}'):
                    self.assertEqual(100, self.offset_and_limit_of_rating_sample.get_arg_limit_of_samples_value)
            self.assertRaises(Exception, exception_context)