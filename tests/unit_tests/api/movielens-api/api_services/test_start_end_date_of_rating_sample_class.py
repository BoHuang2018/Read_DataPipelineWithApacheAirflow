import logging
from typing import List
from unittest import TestCase

import flask

from api.movielens_api.api_services.start_end_date_of_rating_sample_class import StartEndDateOfRatingSample


class TestStartEndDateOfRatingSample(TestCase):
    def setUp(self):
        arg_start_date_key_test = "start_date"
        arg_start_date_default_value_test = '2019-01-01'  # (year-month-day)
        arg_end_date_key_test = "end_date"
        arg_end_date_default_value = '2019-01-02'  # (year-month-day)

        self.start_end_date_of_rating_sample = StartEndDateOfRatingSample(
            arg_start_date_key=arg_start_date_key_test,
            arg_start_date_default_value=arg_start_date_default_value_test,
            arg_end_date_key=arg_end_date_key_test,
            arg_end_date_default_value=arg_end_date_default_value
        )


app = flask.Flask(__name__)


class TestStartEndDateOfRatingSampleWithProperAttributes(TestStartEndDateOfRatingSample):
    start_dates_str_int_dict = {
        "2019-01-01": 1546297200, "2019-01-31": 1548889200, "2020-02-29": 1582930800, "2022-04-12": 1649714400
    }
    end_dates_str_int_dict = {
        "2019-01-02": 1546383600, "2019-02-01": 1548975600, "2020-03-01": 1583017200, "2022-04-13": 1649800800
    }

    def test_date_to_timestamp(self):
        for dd in (self.start_dates_str_int_dict, self.end_dates_str_int_dict):
            for k, v in dd.items():
                date_timestamp_integer = self.start_end_date_of_rating_sample._date_to_timestamp(k)
                self.assertEqual(v, date_timestamp_integer)

    def test_get_arg_start_date_value(self):
        """
        Test the method with given argument 'start_date'
        """
        for k in self.start_dates_str_int_dict.keys():
            with app.test_request_context(f'/ratings?start_date={k}'):
                self.assertEqual(k, self.start_end_date_of_rating_sample._get_arg_start_date_value())
        """
        Test the method without given argument 'start_date', it should return default value, '2019-01-01'
        """
        with app.test_request_context(f'/ratings'):
            self.assertEqual('2019-01-01', self.start_end_date_of_rating_sample._get_arg_start_date_value())

    def test_get_arg_end_date_value(self):
        """
        Test the method with given argument 'end_date'
        """
        for k in self.end_dates_str_int_dict.keys():
            with app.test_request_context(f'/ratings?end_date={k}'):
                self.assertEqual(k, self.start_end_date_of_rating_sample._get_arg_end_date_value())
        """
        Test the method without given argument 'start_date', it should return default value, '2019-01-02'
        """
        with app.test_request_context(f'ratings?'):
            self.assertEqual('2019-01-02', self.start_end_date_of_rating_sample._get_arg_end_date_value())

    def test_start_date_to_timestamp(self):
        """
        Test the method with given argument 'start_date'
        """
        for k, v in self.start_dates_str_int_dict.items():
            with app.test_request_context(f'/ratings?start_date={k}'):
                self.assertEqual(v, self.start_end_date_of_rating_sample.start_date_to_timestamp)
        """
        Test the method without given argument 'start_date', 
        it should return timestamp of default value of 'start_date' (2019-01-01): 1546297200
        """
        with app.test_request_context(f'ratings?'):
            self.assertEqual(1546297200, self.start_end_date_of_rating_sample.start_date_to_timestamp)

    def test_end_date_to_timestamp(self):
        """
        Test the method with given argument 'end_date'
        """
        for k, v in self.end_dates_str_int_dict.items():
            with app.test_request_context(f'/ratings?end_date={k}'):
                self.assertEqual(v, self.start_end_date_of_rating_sample.end_date_to_timestamp)
        """
        Test the method without given argument 'end_date', 
        it should return timestamp of default value of 'start_date' (2019-01-02): 1546383600
        """
        with app.test_request_context(f'ratings?'):
            self.assertEqual(1546383600, self.start_end_date_of_rating_sample.end_date_to_timestamp)


class TestStartEndDateOfRatingSampleWithWrongAttributes(TestStartEndDateOfRatingSample):
    """
    Test the methods will raise exceptions and logs when the attributes are wrong:
    1. String value of 'start_date' or 'end_date' is not in format of "%Y-%m-%d":
        1.1 Year is not in correct position, for example '01-02-2019' ( correct date: '2019-01-02')
        1.2 Month takes date's position while date takes month's position, for example '2019-02-01'
            ( correct date: '2019-01-02', i.e. 02.Jan.2019)
        1.3 Lacking year, month or date
    2. Dates do not existed, for example: '2019-02-29', '2021-01-31', '2020-01-32', '2018-13-01'
    3. Syntax error in string format of date: Characters, decimals, blank space(s) or special characters
    """
    dates_with_errors = {
        "01-01-2019": 1546297200,  # correct version: 2019-01-01 i.e. 01.Jan.2019 --> point 1.1
        "2019--31": 1548889200,  # correct version: 2019-01-31 i.e. 31.Jan.2019 --> point 1.3
        "2019-01-": 1548889200,  # correct version: 2019-01-31 i.e. 31.Jan.2019 --> point 1.3

        "2020-feb-29": 1582930800,  # correct version: 2020-02-29 i.e. 29.Feb.2019 --> point 3
        "2020-#$-29": 1582930800,  # correct version: 2020-02-29 i.e. 29.Feb.2019  --> point 3
        "2020-02-  ": 1582930800  # correct version: 2020-02-29 i.e. 29.Feb.2019 --> point 3
    }
    reversed_month_date_position = {
        "2022-12-04": 1649714400  # correct version: 2022-04-12 i.e. 12.Apr.2022 --> point 1.2
    }
    not_existed_dates: List[str] = ['2019-02-29', '2021-11-31', '2020-01-32', '2018-13-01']  # point 2

    def test_date_to_timestamp_with_wrong_attributes(self):
        for date_string in self.dates_with_errors.keys():
            with self.assertRaises(Exception) as context:
                self.start_end_date_of_rating_sample._date_to_timestamp(date_string)
            self.assertEqual(str(context.exception),
                             f"failed to get integer of date={date_string} in format of %Y-%m-%d")
            self.assertEqual(type(context.exception).__name__, 'ValueError')

        for date_string in self.not_existed_dates:
            with self.assertRaises(Exception) as context:
                self.start_end_date_of_rating_sample._date_to_timestamp(date_string)
            self.assertEqual(str(context.exception),
                             f"failed to get integer of date={date_string} in format of %Y-%m-%d")
            self.assertEqual(type(context.exception).__name__, 'ValueError')
