# import pandas as pd
# from functools import wraps
# from dataclasses import dataclass
# from typing import List, Dict

from scorpion.util_classes import GenericManager, singleton


class DataFrameManagerError(Exception):
    pass


@singleton
class DataFrameManager(GenericManager):

    exception = DataFrameManagerError

    def __init__(self):
        super().__init__()



    # @property
    # def _data(self):
    #     return self._data_
    #
    # def __iter__(self):
    #     for key in self._data_.keys():
    #         yield key, self._data_[key]
    #
    # def __getitem__(self, item):
    #     return self._data_[item]
    #
    # def __setitem__(self, key, value):
    #     pass
    #
    # def __contains__(self, item):
    #     pass
    #
    # def check_data_integrity(self):
    #     pass

# def keys_is_available(f):
#     @wraps(f)
#     def wrapper(self, *args, **kwargs):
#         try:
#             result = f(self, *args, **kwargs)
#         except KeyError as err:
#             raise DataFrameManagerError(f'Data frame manager is missing key "{err}"') from err
#         else:
#             return result
#     return wrapper


# @dataclass
# class DF:
#     key: str
#     df: pd.DataFrame


# class DFManager:
#     def __init__(self):
#         self._data_frames = {}
#
#     def __contains__(self, item):
#         if item not in self._data_frames:
#             raise KeyError(f'Key "{item} is not in {self.__class__.__qualname__}')
#         return True
#
#     # def __getitem__(self, df_key):
#     #     _ = df_key in self
#     #     return self._data_frames[df_key]
#
#     @property
#     def data_frames(self):
#         return self._data_frames
#
#     def add_data_frames_from_sources(self, sources) -> None:
#         for source in sources:
#             self.add_data_frame(source.key, source.df)
#
#     # @keys_is_available
#     def get_data_frame(self, df_key: str) -> DF:
#         if df_key not in self: pass
#         return self._data_frames[df_key]
#
#     # @keys_is_available
#     def get_data_frames(self, df_keys: List[str]) -> Dict[str, DF]:
#         return {df_key: self.get_data_frame(df_key) for df_key in df_keys}
#
#     def add_data_frame(self, df_key: str, df: DF) -> None:
#         self._add_df_to_container(self._data_frames, df_key, df)
#
#     def add_data_frames(self, data_frames: Dict[str, DF]) -> None:
#         for df_key, df in data_frames.items():
#             self.add_data_frame(df_key, df)
#
#     def _add_df_to_container(self, df_container, df_key, df) -> None:
#         # if df_key in df_container:
#         #     raise DataFrameManagerError(f'Key "{df_key}" must be unique in {df_container}')
#         df_container[df_key] = df
