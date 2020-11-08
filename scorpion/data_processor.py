import re
import pandas as pd
import abc
from typing import List, Dict

from src.utils import AutoReprMixin, vlookup


class DataProcessorError(Exception):
    pass


class DataProcessor(abc.ABC, AutoReprMixin):
    key = None

    def __init__(self):

        if self.key is None:
            raise NotImplementedError('Key in DataProcessor must be set')

        self._input_data_frames = None
        self._expected_output_data_frames = None
        self._output_data_frames = {}

    def add_input_data_frames(self, df_input: Dict[str, pd.DataFrame]) -> None:
        self._input_data_frames = df_input

    def set_expected_output_data_frames(self, expected_return_data_frames: List[str]) -> None:
        self._expected_output_data_frames = expected_return_data_frames

    def get_data_frame_by_key(self, key) -> pd.DataFrame:
        try:
            df = self._input_data_frames[key]
        except KeyError as err:
            raise DataProcessorError(
                f'Key "{key}" is not available in data processor "{self.key};'
                f'\nKeyError occurred in {self.get_data_frame_by_key}"') from err
        else:
            return df

    @abc.abstractmethod
    def process(self) -> None:
        pass

    @property
    def output(self) -> Dict[str, pd.DataFrame]:
        for df_key in self._expected_output_data_frames:
            if df_key not in self._output_data_frames:
                raise DataProcessorError(
                    f'Data frame output key "{df_key}" is defined in config as expected output dataframe key,'
                    f'\nbut key "{df_key}" is not found as output key in output for DataProcessor "{self.key}";'
                    f'\nDataProcessor "{self.key}" only knows these output data frame keys "{",".join([key for key in self._output_data_frames])}"'
                )
        return self._output_data_frames

    def add_data_frame_to_output(self, key: str, df: pd.DataFrame) -> None:
        if key in self._output_data_frames:
            raise KeyError(f'Key "{key}" cannot be used more than once in "{self.key}"')
        self._output_data_frames[key] = df





data_processors = [

]
