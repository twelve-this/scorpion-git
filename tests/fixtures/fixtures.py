import pytest

import scorpion.utils
import scorpion.config
import scorpion.util_classes


class GenericManagerTestException(Exception):
    pass


class GenericManagerTestableChild(scorpion.util_classes.GenericManager):
    exception = GenericManagerTestException


@pytest.fixture
def config_from_json():
    return scorpion.utils.load_config('fixtures/data/config_file.json', format_='json')


@pytest.fixture
def config_from_yaml():
    return scorpion.utils.load_config('fixtures/data/config_file.yaml', format_='yaml')


@pytest.fixture
def config_from_yaml_skip_first_level():
    return scorpion.utils.load_config(
        'fixtures/data/config_file.yaml',
        format_='yaml',
        skip_first_level=True,
        first_level_key='config',
    )


@pytest.fixture
def config_manager_object_valid(config_from_yaml):
    config = config_from_yaml
    return scorpion.config.Config(config)


@pytest.fixture
def generic_manager():
    return GenericManagerTestableChild()
