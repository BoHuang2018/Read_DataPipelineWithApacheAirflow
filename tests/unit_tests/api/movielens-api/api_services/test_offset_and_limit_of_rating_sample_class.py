from unittest import TestCase

import flask
from api.movielens_api.api_services.offset_and_limit_of_rating_sample_class import OffsetAndLimitOfRatingSample


class TestOffsetAndLimitOfRatingSample(TestCase):
    def setUp(self):
        arg_offset_of_samples_key_test = "offset"
        arg_offset_of_samples_default_value = 0
        arg_limit_of_samples_key = "limit"
        arg_limit_of_samples_default_value = 100

        self.offset_and_limit_of_rating_sample = \
            OffsetAndLimitOfRatingSample(
                arg_offset_of_samples_key_test,
                arg_offset_of_samples_default_value,
                arg_limit_of_samples_key,
                arg_limit_of_samples_default_value
            )


class TestOffsetAndLimitOfRatingSampleMethodGetArgOffsetOfSamplesValue(TestOffsetAndLimitOfRatingSample):
    def test_get_arg_offset_of_samples_value(self):
        app = flask.Flask(__name__)
        with app.test_request_context('/ratings?offset=100'):
            self.assertEqual(100, self.offset_and_limit_of_rating_sample.get_arg_offset_of_samples_value)