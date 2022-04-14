import logging
import time
from dataclasses import dataclass

from flask import request


@dataclass(frozen=True)
class StartEndDateOfRatingSample:
    """
    This class is to get start date's timestamp and end date's timestamp (of ratings sample) in format of integer
    :attr: arg_start_date_key: str: name of argument bearing value of start date of ratings sample
    :attr: arg_start_date_default_value: str:
                default value of start date of ratings sample, for example '2019-01-01' (year-month-day)
    :attr: arg_end_date_key: str: name of argument bearing value of end date of ratings sample
    :attr: arg_end_date_default_value: str:
                default value of end date of ratings sample, for example '2019-01-02' (year-month-day)
    """
    arg_start_date_key: str
    arg_start_date_default_value: str
    arg_end_date_key: str
    arg_end_date_default_value: str

    @staticmethod
    def _date_to_timestamp(date_str: str) -> int:
        """
        :param date_str: timestamp of date, in string, in format "%Y-%m-%d"
        :return integer of the date_str
        """
        if date_str is None:
            raise ValueError("argument data_str cannot be None")
        try:
            date_int = int(time.mktime(time.strptime(date_str, "%Y-%m-%d")))
        except ValueError:
            raise ValueError(f"failed to get integer of date={date_str} in format of %Y-%m-%d")
        except Exception as e:
            logging.error(f"failed to get integer of date in format of %Y-%m-%d, because of unexpected error: {e=}")
            raise e
        return date_int

    def _get_arg_start_date_value(self) -> str:
        try:
            arg_start_date_value = request.args.get(self.arg_start_date_key, self.arg_start_date_default_value)
        except Exception as e:
            logging.error(f"failed to get value of argument of {self.arg_start_date_key}")
            raise e
        return arg_start_date_value

    def _get_arg_end_date_value(self) -> str:
        try:
            arg_end_date_value = request.args.get(self.arg_end_date_key, self.arg_end_date_default_value)
        except Exception as e:
            logging.error(f"failed to get value of argument of {self.arg_end_date_key}")
            raise e
        return arg_end_date_value

    @property
    def start_date_to_timestamp(self) -> int:
        arg_start_date_value = self._get_arg_start_date_value()
        if arg_start_date_value is None:
            raise ValueError(f"value of argument {self.arg_start_date_key} cannot be None")
        return self._date_to_timestamp(arg_start_date_value)

    @property
    def end_date_to_timestamp(self) -> int:
        arg_end_date_value = self._get_arg_end_date_value()
        return self._date_to_timestamp(arg_end_date_value)
