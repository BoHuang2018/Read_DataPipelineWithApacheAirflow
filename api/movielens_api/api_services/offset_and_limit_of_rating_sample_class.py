import logging
from dataclasses import dataclass

from flask import request


@dataclass(frozen=True)
class OffsetAndLimitOfRatingSample:
    """
    :attr: arg_offset_of_samples_key: str:
            Name of argument involving amount of next set of samples will be retrieved
            For example, 100 (integer), it says we will get next 100 ratings samples
    :attr: arg_offset_of_samples_default_value: int:
            Default value of the argument, arg_offset_of_samples_key
    :attr: arg_limit_of_samples_key: str:
            Name of argument involving amount of total number of samples will be retrieved in maximum
            For example, 1000 (integer), it says we will get 1000 ratings samples in maximum
    :attr: arg_limit_of_samples_default_value: int:
            Default value of the argument, arg_limit_of_samples_key
    """
    arg_offset_of_samples_key: str
    arg_offset_of_samples_default_value: int
    arg_limit_of_samples_key: str
    arg_limit_of_samples_default_value: int

    @property
    def get_arg_offset_of_samples_value(self) -> int:
        try:
            value_str = request.args.get(self.arg_offset_of_samples_key, self.arg_offset_of_samples_default_value)
            value_int = int(value_str)
        except Exception as e:
            logging.error(f"failed to get integer value of argument {self.arg_offset_of_samples_key} "
                          f"with default value {self.arg_offset_of_samples_default_value}")
            raise e
        return value_int

    @property
    def get_arg_limit_of_samples_value(self) -> int:
        try:
            value_str = request.args.get(self.arg_limit_of_samples_key, self.arg_limit_of_samples_default_value)
            value_int = int(value_str)
        except Exception as e:
            logging.error(f"failed to get integer value of argument {self.arg_limit_of_samples_key} "
                          f"with default value {self.arg_limit_of_samples_default_value}")
            raise e
        return value_int
