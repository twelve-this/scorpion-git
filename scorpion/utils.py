import re
import json
import yaml
import itertools
from functools import singledispatch, reduce
from collections import Counter, namedtuple
from dataclasses import dataclass
from keyword import iskeyword
import pandas as pd
from typing import Tuple, Iterable, Hashable, Union, List, Dict, Any, Mapping


KeyPair = Tuple[Hashable, Hashable]


def raise_key_error_if_missing_key(mapping, keys: Iterable):
    for key in keys:
        if key not in mapping:
            raise KeyError(
                f'Key "{key}" was not found;'
                f'\n these keys were provided: {keys}'
                f'\n This mapping was provided: {mapping}'
            )


def rename_keys(
        mapping: Mapping,
        key_pairs: Union[KeyPair, List[KeyPair]],
        keep_keys_old: bool = False,
        silent_key_error: bool = False,
        unsafe_keys_old: bool = False,
        unsafe_keys_new: bool = False,
        inplace: bool = False,
) -> Dict:

    @singledispatch
    def key_pairs_as_named_tuple(keys):
        return keys

    @key_pairs_as_named_tuple.register
    def _(keys: tuple):
        return key_pair(keys[0], keys[1])

    @key_pairs_as_named_tuple.register
    def _(keys: list):
        return [key_pair(key[0], key[1]) for key in keys]

    @singledispatch
    def key_pairs_to_list(key_pairs):
        return key_pairs

    @key_pairs_to_list.register
    def _(key_pairs: tuple):
        return [key_pairs]

    @key_pairs_to_list.register
    def _(key_pairs: list):
        return key_pairs

    def rename(mapping, key_pairs):
        key_pairs_as_dict = {key_pair.old: key_pair.new for key_pair in key_pairs}
        get_key_new_if_available = lambda key_old: (
            key_pairs_as_dict[key_old]
            if key_old in key_pairs_as_dict
            else key_old
        )
        mapping_new = {get_key_new_if_available(key): value for key, value in mapping.items()}
        mapping_keys_old = lambda: {key_pair.old: mapping[key_pair.old] for key_pair in key_pairs}
        merge_mapping_new_with_keys_old = lambda: {**mapping_new, **mapping_keys_old()}
        return mapping_new if not keep_keys_old else merge_mapping_new_with_keys_old()

    def check(mapping, key_pairs):
        if not silent_key_error:
            keys_old_as_set = set(key_pair.old for key_pair in key_pairs)
            raise_key_error_if_missing_key(mapping, keys_old_as_set)

        if not unsafe_keys_old:
            old_keys_count = Counter([key_pair.old for key_pair in key_pairs])
            old_keys_count_gt_1 = [key for key, value in old_keys_count.items() if value > 1]
            if any(old_keys_count_gt_1):
                ambiguous_keys = ', '.join(old_keys_count_gt_1)
                raise KeyError(f'The following keys are ambiguous: {ambiguous_keys}')

        if not unsafe_keys_new:
            equal_key = lambda key_pair: key_pair.old == key_pair.new
            equal_keys = Counter([key_pair for key_pair in key_pairs if equal_key(key_pair)])
            if any(equal_keys):
                equal_keys_joined = ', '. join(equal_keys)
                raise KeyError(f'Old key is equal to new key: {equal_keys_joined}')

    key_pair = namedtuple('key_pair', 'old new')
    key_pairs = key_pairs_as_named_tuple(key_pairs)
    key_pairs_as_list = key_pairs_to_list(key_pairs)

    check(mapping, key_pairs_as_list)

    return rename(mapping, key_pairs_as_list)


def filter_mapping(
        mapping: Mapping[Hashable, Any],
        filter_keys: List[Hashable],
        filter_method: str,
        silent_key_error: bool = False,
) -> Dict[Hashable, Any]:
    """
    Filter mapping using filter keys and return new dictionary from filtered mapping.

    Parameters
    ----------
    mapping:
        Mapping which is subject to filtering. Function does not mutate mapping.
    filter_keys:
        Keys which are used for filtering.
    filter_method:
        Using method "keep", a key which is in filter_keys will be kept in mapping.
        Using method "drop", a key which is in filter_keys will be dropped from mapping.
        If filter_keys is an empty list while using method "keep", all keys are dropped from mapping.
        If filter_keys is an empty list while using method "drop", no key is dropped from mapping.
    silent_key_error:
        If set to False, KeyError is raised if a key from filter_keys is not in mapping.
        If set to True, KeyError is silenced if key from filter_keys is not in mapping.
        Default is False.

    Raises
    ------
        ValueError
            If called with unsupported filter method.
        KeyError
            If silence_key_error is set to False and key from filter_keys is not in mapping.

    Returns
    ------
        A new dictionary which was filtered according to filter method
    """

    filter_keys_set = set(filter_keys)
    key_is_in_keys_set = lambda k: k in filter_keys_set
    filter_methods = {
        'keep': (filter(key_is_in_keys_set, mapping)),
        'drop': (itertools.filterfalse(key_is_in_keys_set, mapping)),
    }

    if filter_method not in filter_methods:
        raise ValueError(
            f"""Filter method "{filter_method}" is not supported."""
            f"""\nThese filter methods are supported: {", ".join(filter_methods)}."""
        )

    if not silent_key_error:
        raise_key_error_if_missing_key(mapping, filter_keys_set)

    return {key: mapping[key] for key in filter_methods[filter_method]}


@dataclass
class _ContainerRelationshipReport:
    """
    Is helper class and is returned by analyze container relationship.

    Is not supposed to be directly instantiated
    """
    left: Iterable
    right: Iterable
    shared: List
    only_left: List
    only_right: List

    @property
    def left_is_subset_of_right(self) -> bool:
        return len(self.only_left) == 0

    @property
    def right_is_subset_of_left(self) -> bool:
        return len(self.only_right) == 0


def analyze_container_relationship(left: Iterable, right: Iterable) -> _ContainerRelationshipReport:
    """
    Analyze which elements of left container are in right container;
    Analyze which elements of right container are in left container.

    Return ContainerRelationshipReport in order to document result
    Warning: do not use with large Iterables because this function is not optimized to handle large Iterables
    """
    shared = []
    only_left = []
    only_right = []

    # TODO check if low level implementation can be modified to higher level implementation using functions
    for left_item in left:

        if left_item in right:
            shared.append(left_item)
        else:
            only_left.append(left_item)

    for right_item in right:

        if right_item in shared:
            continue

        if right_item in left:
            shared.append(right_item)
        else:
            only_right.append(right_item)

    return _ContainerRelationshipReport(
        left=left,
        right=right,
        shared=shared,
        only_left=only_left,
        only_right=only_right,
    )


def transform_to_valid_attr_name(s: str) -> str:
    """Transform string to a valid attribute name for names in dynamic attribute creation."""
    pattern = r"""
        (^[^a-zA-Z_]|\W)  # Valid Python attr name may only begin with [a-zA-Z_];
                          # Special characters in the middle of the string are not allowed as well
        """
    replacement = '_'  # Replacement for invalid characters

    functions = [
        lambda s: s.strip(),  # Remove left and right white space
        lambda s: f'{s}_' if iskeyword(s) else s,  # Append underscore if s is Python keyword to avoid SyntaxError
        lambda s: re.sub(pattern, replacement, s, flags=re.VERBOSE),  # Replace pattern with replacement
    ]

    # Apply functions: one after another, in the order of the functions list
    return reduce(lambda f, g: lambda x: g(f(x)), functions)(s)


def produce_mapping_with_valid_attr_names(mapping: Mapping[str, Any]) -> Dict[str, Any]:
    # TODO needs unittest
    # TODO needs docstring
    return {transform_to_valid_attr_name(k): v for k, v in mapping.items()}


def load_config(
        file_path: str,
        format_: str,
        skip_first_level: bool = False,
        first_level_key: str = None,
) -> Dict[str, Any]:
    # TODO needs docstring
    # TODO check if change to enum for loaders is a better way
    # TODO check if other Loader can be used so that a tuple can be retrieved rather than a Dict
    loaders = {
        'json': lambda file: json.load(file),
        'yaml': lambda file: yaml.safe_load(file),
    }

    if skip_first_level and first_level_key is None:
        raise TypeError(f'First level key needs to be provided if first level is supposed to be skipped')

    if format_ not in loaders:
        raise ValueError(
            f"""Format {format_} is not supported.
            These formats are supported: "{','.join(loaders)}"."""
        )
    else:
        loader = loaders[format_]
        with open(file_path) as config_file:
            return loader(config_file) if not skip_first_level else loader(config_file)[first_level_key]

# def get_values_to_key_from_list_of_dict(list_of_dict: List[Dict], key: str) -> List[Any]:
#     return [dict_[key] for dict_ in list_of_dict]
#
#
# def items_unique_in_container(
#         container: Iterable,
#         exception,
#         container_name: str = None,
#         extra_message: str = None
# ) -> None:
#     cached = {}
#     for item in container:
#         if item in cached:
#             raise exception(
#                 f'Item "{item}" is not unique'
#                 + (f' in {container_name}' if container_name is not None else '')
#                 + (f'\n{extra_message}' if extra_message is not None else '')
#             )
#         cached[item] = None
#
#
#
#
# class DFManagerMixin:
#
#     def __init__(self):
#         self._df_manager = None
#
#     def add_df_manager(self, df_manager):
#         self._df_manager = df_manager
#
#
# def one_in_the_other_and_vc(
#         one,
#         other,
#         exception_one,
#         message_one=None,
#         exception_other=None,
#         message_other=None,
# ):
#     if exception_other is None:
#         exception_other = exception_one
#
#     if message_other is None:
#         message_other = message_one
#
#     for item in one:
#         if item not in other:
#             err = f'"{item}" not in {other}' + f'\n{message_other}' if message_one is not None else ''
#             raise exception_other(err)
#
#     for item in other:
#         if item not in one:
#             err = f'"{item}" not in {one}' + f'\n{message_one}' if message_one is not None else ''
#             raise exception_one(err)
#
#
#
#
#
# def vlookup(*,
#             keys: Iterable[Hashable],
#             df_target: pd.DataFrame,
#             df_target_column_name: str,
#             na_text: str = 'n/a',
#             output_format='list') -> Union[List, pd.Series, pd.DataFrame]:
#     def as_list(result):
#         return result
#
#     def as_pd_series(result):
#         return pd.Series(data=result)
#
#     def as_pd_data_frame(result):
#         return pd.DataFrame(data=result)
#
#     def result_factory(result, factory):
#         factories = {
#             'list': as_list,
#             'series': as_pd_series,
#             'data_frame': as_pd_data_frame,
#         }
#         return factories[factory](result)
#
#     target_values = []
#     for key in keys:
#         value = None
#         try:
#             value = df_target.loc[key, df_target_column_name]
#         except KeyError:
#             value = na_text
#         finally:
#             if value is not None:
#                 target_values.append(value)
#
#     return result_factory(target_values, output_format)
