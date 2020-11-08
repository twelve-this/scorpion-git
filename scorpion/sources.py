import os.path
from dataclasses import dataclass
import pandas as pd
from collections import ChainMap
from typing import List, Hashable, Dict, Any

from scorpion.util_classes import GenericManager, singleton, ConfigMixin
from scorpion.utils import (
    analyze_container_relationship,
    transform_to_valid_attr_name,
    filter_mapping,
)


# TODO Source Manager manages Sources by key
# TODO Source manager checks integrity of source config
# TODO Introduce class SourceLoader which handles to loading for the sources
# TODO Introduce class SourceConfigLoader which handles the source configuration and the override; use ChainMap; returns clean SourceConfig object which is only a dataclass of clean values; no logic
# TODO Sources get loaded as pandas df
# TODO Sources exec functions in order to bring the df up to spec, e.g. change column names


class SourceManagementError(Exception): pass


class SourceFileLoader:

    supported_formats = ['csv', 'excel']

    def __init__(self, **kwargs):

        # self.file_name = kwargs['file_name']
        # self.format = kwargs['format']
        # self.sheet_name = kwargs['sheet_name']
        # self.encoding = kwargs['encoding']
        # self.delimiter = kwargs['delimiter']
        # self.nrows = kwargs['nrows']

        self._kwargs = kwargs
        self._check_attributes()

    def _check_attributes(self):

        if self._kwargs['format'] not in self.supported_formats:
            raise SourceManagementError(
                f'{self.__class__.__qualname__} does not support given format {self._kwargs["format"]}'
            )

    def _filter_kwargs_for_loader(self, format_):

        kwargs = {
            'csv': filter_mapping(self._kwargs, ['sheet_name'], 'drop', silent_key_error=True),
            'excel': filter_mapping(self._kwargs, ['encoding', 'delimiter'], 'drop', silent_key_error=True),
        }

        return kwargs[format_]

    @classmethod
    def load(cls, **kwargs):

        source_file_loader = cls(**kwargs)
        kwargs = source_file_loader._filter_kwargs_for_loader(source_file_loader._kwargs['format'])
        return pd.read_csv(**kwargs)

class Source:
    pass


class SourceConfig:
    # Keys which are removed from config Dict in order to have a clean Config which only contains
    # relevant keys for the next processing step
    _keys_for_removal = ['priority']

    def __init__(
            self,
            source_name: str,
            default_priority: str,
            config_default: Dict[str, Any] = None,
            config_source: Dict[str, Any] = None,
            required_config_items_in_source: List[str] = None,

    ) -> None:
        self.source_name = source_name

        self.default_priority = default_priority

        self.config_default = (
            config_default
            if config_default is not None
            else {}
        )

        self.config_source = (
            config_source
            if config_source is not None
            else {}
        )

        self.required_config_items_in_source = (
            required_config_items_in_source
            if required_config_items_in_source is not None
            else {}
        )

        self.required_config_items_in_source = \
            list(map(transform_to_valid_attr_name, self.required_config_items_in_source))

    @property
    def source_priority(self):
        return self.config_source['priority']

    @property
    def priority(self):
        return (
            'source'
            if self.source_priority == 'source' and self.default_priority == 'source'
            else 'default'
        )

    @property
    def config(self) -> Dict[Hashable, Any]:
        self._check_source_config()
        config_unfiltered = self._calc_config()
        return filter_mapping(config_unfiltered, self._keys_for_removal, filter_method='drop')

    def _calc_config(self) -> Dict[str, Any]:
        return dict(
            ChainMap(self.config_source, self.config_default)
            if self.priority == 'source'
            else ChainMap(self.config_default, self.config_source)
        )

    def _check_source_config(self):
        result = analyze_container_relationship(self.required_config_items_in_source, self.config_source)
        if not result.left_is_subset_of_right:
            raise SourceManagementError(
                f"""Source "{self.source_name}" does not contain all required config items;"""
                f"""\nsource "{self.source_name}" has these config items: {", ".join(self.config_source)};"""
                f"""\nHowever, these config items are required: {", ".join(self.required_config_items_in_source)};"""
                f"""\nMissing config items: {", ".join(result.only_left)}"""
            )


@singleton
class SourceManager(GenericManager, ConfigMixin):
    exception = SourceManagementError

    def __init__(self):
        super().__init__()

    def prepare_sources(self) -> Dict[str, Any]:
        for source_name, source in self.config.sources.data:
            source_config = SourceConfig(
                source_name=source_name,
                default_priority=self.config.sources.priority,
                config_default=self.config.sources.defaults.as_dict,
                config_source=source.as_dict,
                required_config_items_in_source=self.config.sources.required_config_items_in_source).config

            file_handle = SourceFileLoader.load(**source_config)
            print(file_handle)


        return self.data

    def _prepare_source_config(self, source_config):
        pass

    def _load_source_file(self, path):
        pass

    def _load_source(self, source_raw, source_config):
        pass

# class SourceKeysNotUniqueError(Exception):
#     pass
#
#
# class SourceFormatNotSupportedError(Exception):
#     pass
#
#
# class ConfigMismatchError(Exception):
#     pass


#
# @dataclass
# class Column:
#     name: str
#     new_name: str
#
#
# class Source(AutoReprMixin):
#
#     SUPPORTED_FORMATS = ['excel', 'csv']
#
#     def __init__(self,
#                  *,
#                  key,
#                  relative_path_to_source,
#                  local_override_items,
#                  specification):
#         self.key = key
#         self._relative_path_to_source = relative_path_to_source
#
#         self._config = self._setup_config(specification, local_override_items)
#
#         self._path_to_source = self._create_path(self._relative_path_to_source, self.get_config_item('file_name'))
#         self._columns = self._create_columns(self.get_config_item('columns'))
#         self.df = self._to_df()
#         self._rename_columns()
#
#     def get_config_item(self, key: str) -> Any:
#         return self._config[key].value
#
#     def _setup_config(self, specification, local_override_items):
#         config = {}
#         local_config_items = self._create_local_config_items(specification['local_config_items'])
#         self._check_config_integrity(local_config_items, local_override_items)
#         self._unique_keys(local_config_items, local_override_items)
#         config.update(self._override_config_items(local_config_items, local_override_items))
#         config.update(self._prepare_non_override_config_items(specification))
#         return config
#
#     def _unique_keys(self, local_config_items, local_override_items):
#         pass
#
#     def _prepare_non_override_config_items(self, specification):
#         return {key: ConfigItem(key, value) for key, value in specification.items() if key != "local_config_items"}
#
#     def _override_config_items(self,
#                                local_config_items: Dict[str, LocalConfigItem],
#                                local_override_items: Dict[str, ConfigOverrideItem]) \
#             -> Dict[str, ConfigItem]:
#         return {key: local_config_item.create_config_item_from_override(local_override_items[key])
#                 for key, local_config_item in local_config_items.items()}
#
#     def _create_local_config_items(self, local_config_items_raw: List[Dict[str, Any]]) -> Dict[str, LocalConfigItem]:
#         return {config_item['key']: LocalConfigItem(
#             key=config_item['key'],
#             value=config_item['value'],
#             local_override=config_item['local_override']
#         ) for config_item in local_config_items_raw}
#
#     def _check_config_integrity(self, local_config_items: Dict, local_override_items: Dict) -> None:
#
#         for local_config_item in local_config_items.keys():
#             if local_config_item not in local_override_items.keys():
#                 raise ConfigMismatchError(
#                     f'"{local_config_item}" is a local config item, but no local override item was provided\n'
#                     f'Affected source: {repr(self)}')
#
#         for local_override_item in local_override_items.keys():
#             if local_override_item not in local_config_items.keys():
#                 raise ConfigMismatchError(
#                     f'"{local_override_item}" is a local override item, but no local config item was provided\n'
#                     f'Affected source: {repr(self)}')
#
#     def _set_local_config_items(self, local_config_items):
#         pass
#
#     @property
#     def columns(self) -> Dict[str, Column]:
#         return self._columns
#
#     @property
#     def column_names(self) -> List[str]:
#         return [column.name for column in self._columns.values()]
#
#     def _create_columns(self, columns_raw: List[Dict]) -> Dict[str, Column]:
#         return {column['name']: Column(
#             name=column['name'],
#             new_name=column['new_name'])
#             for column in columns_raw}
#
#     @property
#     def column_name_new_name_map(self):
#         return {column.name: column.new_name for column in self.columns.values()}
#
#     def _rename_columns(self):
#         if self.get_config_item('allow_rename_columns'):
#             self.df.rename(columns=self.column_name_new_name_map, inplace=True)
#
#     def _create_path(self, relative_path, source_file):
#         return os.path.join(relative_path, source_file)
#
#     def _check_format_support(self) -> None:
#         if self.get_config_item('format') not in Source.SUPPORTED_FORMATS:
#             raise SourceFormatNotSupportedError(
#                 f"""Source format "{self.get_config_item('format')}" is not supported""")
#
#     def _to_df(self) -> pd.DataFrame:
#
#         self._check_format_support()
#
#         io = self._path_to_source
#         usecols = self.column_names if self.get_config_item('limit_columns') else None
#         nrows = self.get_config_item('nrows') if self.get_config_item('limit_nrows') else None
#
#         if self.get_config_item('format') == 'excel':
#             return pd.read_excel(
#                 io=io,
#                 usecols=usecols,
#                 nrows=nrows,
#                 sheet_name=self.get_config_item('sheet_name'),
#                 index_col=False,
#             )
#
#         elif self.get_config_item('format') == 'csv':
#             return pd.read_csv(
#                 filepath_or_buffer=io,
#                 usecols=usecols,
#                 nrows=nrows,
#                 encoding=self.get_config_item('encoding'),
#                 delimiter=self.get_config_item('delimiter'),
#                 index_col=False,
#             )
#
#
# class SourceManager:
#
#     def __init__(self,
#                  *,
#                  relative_path_to_sources: str,
#                  local_override_items: Dict,
#                  sources_raw: List[Dict]):
#         self._relative_path_to_sources = relative_path_to_sources
#
#         self._local_override_items = local_override_items
#
#         self._sources_raw = self._remove_skipped(sources_raw)
#
#         self._source_keys = self._collect_source_keys(self._sources_raw)
#         self._source_keys_are_unique()
#         self.sources = self._load(self._sources_raw)
#
#     def __getitem__(self, source_key) -> Source:
#         return self.sources[source_key]
#
#     def __setitem__(self, key, value):
#         pass
#
#     def __iter__(self):
#         for key in self._source_keys:
#             yield self.sources[key]
#
#     def _remove_skipped(self, sources_raw: List[Dict]) -> List[Dict]:
#         return [source_raw for source_raw in sources_raw if not source_raw['source_specification']['skip']]
#
#     @property
#     def amount_sources_raw(self) -> int:
#         return len(self._sources_raw)
#
#     @property
#     def amount_unique_source_keys(self) -> int:
#         return len(set(self._source_keys))
#
#     def _collect_source_keys(self, sources_raw) -> List[str]:
#         return [source_raw['source_key'] for source_raw in sources_raw]
#
#     def _source_keys_are_unique(self) -> None:
#         if self.amount_unique_source_keys != self.amount_sources_raw:
#             raise SourceKeysNotUniqueError(f'Source keys in "{self.__class__.__name__}" need to be unique')
#
#     def _load(self, sources_raw) -> Dict[str, Source]:
#         return {source_raw['source_key']:
#             Source(
#                 key=source_raw['source_key'],
#                 relative_path_to_source=self._relative_path_to_sources,
#                 local_override_items=self._local_override_items,
#                 specification=source_raw['source_specification'])
#             for source_raw in sources_raw}
