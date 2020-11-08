from collections import abc
from typing import Tuple, Iterable

from scorpion.util_classes import auto_repr
from scorpion.utils import produce_mapping_with_valid_attr_names


@auto_repr
class Config:

    def __init__(self, mapping: abc.Mapping) -> None:
        self.__data = produce_mapping_with_valid_attr_names(mapping)

    def __getattr__(self, name: str) -> 'Config':
        if hasattr(self.__data, name):
            return getattr(self.__data, name)
        else:
            try:
                data = Config.build(self.__data[name])
            except KeyError as err:
                raise AttributeError(
                    f'Attribute "{name}" is not found in {self.__class__.__qualname__}'
                ) from None
            else:
                return data

    def __iter__(self) -> Iterable[Tuple[str, 'Config']]:
        for item in self.__data:
            yield item, Config.build(self.__data[item])

    @classmethod
    def build(cls, obj):
        if isinstance(obj, abc.Mapping):
            return cls(obj)
        elif isinstance(obj, abc.MutableSequence):
            # TODO This returns a list of ConfigManager, not a ConfigManager; therefore Configmanager does not raise IndexError, but low-level list does. Check, if this needs to changed so that Configmanager raises IndexError and reroutes to AttributeError
            return [cls.build(item) for item in obj]
        else:
            return obj

    @property
    def as_dict(self):
        return self.__data
