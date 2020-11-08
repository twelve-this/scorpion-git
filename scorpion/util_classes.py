from abc import ABC, abstractmethod
import string
from typing import Tuple, List, Dict, Any


def singleton(cls):
    # TODO needs tests
    # TODO needs docstring
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return get_instance


def auto_repr(cls):
    # TODO needs unittest
    # TODO needs docstring

    def repr(self):
        items = ('{!s}={!r}'.format(k, v) for k, v in self.__dict__.items())
        return '{!s}({!s})'.format(self.__class__.__qualname__, ', '.join(items))

    cls.__repr__ = repr
    cls.__str__ = cls.__repr__
    return cls


# class AutoReprMixin:
#
#     def __repr__(self) -> str:
#         items = ('{!s}={!r}'.format(k, v) for k, v in self.__dict__.items())
#         return '{!s}({!s})'.format(self.__class__.__qualname__, ', '.join(items))
#
#     __str__ = __repr__


class ConfigMixin:
    # TODO needs unittest
    # TODO needs docstring

    def __init__(self):
        self._config = None

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = config


class DataIntegrityCheckMixin(ABC):
    # TODO needs unittest
    # TODO needs docstring

    @abstractmethod
    def check_data_integrity(self):
        pass


@auto_repr
class GenericManager:
    # TODO needs unittest
    # TODO needs docstring

    exception = NotImplementedError
    exception_message__get_item__ = string.Template('Key "$key" is not found')
    exception_message__set_item__ = string.Template('Key "$key" is already set and cannot be reset')

    def __init__(self) -> None:
        super().__init__()
        self._data = {}

    def __iter__(self) -> Tuple[str, Any]:
        for key in self._data:
            yield key, self._data[key]

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, key: str) -> Any:
        if key not in self:
            raise self.exception(self.exception_message__get_item__.safe_substitute(key=key))
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        if key in self:
            raise self.exception(self.exception_message__set_item__.safe_substitute(key=key))
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._data

    __eq__ = None

    __hash__ = None

    @property
    def data(self):
        return self._data

    def get_multiple_items(self, keys: List[str]) -> Dict[str, Any]:
        return {key: self[key] for key in keys}

    def set_multiple_items(self, items: Dict[str, Any]) -> None:
        for key, value in items.items():
            self[key] = value


