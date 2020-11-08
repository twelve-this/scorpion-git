import pytest

from fixtures.fixtures import config_manager_object_valid, config_from_yaml
# TODO config_from_yaml is imported for the fixture "config_manager_object_valid; check if fixtures can be combined otherwise


class TestConfigManager:
    # TODO use setup method before each test case because every test case loads a new config_manager

    def test_attr_access(self, config_manager_object_valid):
        config_manager = config_manager_object_valid
        assert config_manager.config.global_[0].nrows == 10
        assert config_manager.config.sources[1].key == 'source_2'

    @pytest.mark.parametrize(
        "attr_name",
        [
            'config.if',
            'config.global',
        ]
    )
    def test_syntax_error(self, config_manager_object_valid, attr_name):
        config_manager = config_manager_object_valid
        with pytest.raises(SyntaxError):
            _ = eval(f'config_manager.{attr_name}')

    @pytest.mark.parametrize(
        "attr_name",
        [
            'config.attr',
            'config.global_._attr45'
        ]
    )
    def test_attr_error_if_attribute_error(self, config_manager_object_valid, attr_name):
        config_manager = config_manager_object_valid
        with pytest.raises(AttributeError):
            _ = eval(f'config_manager.{attr_name}')

    @pytest.mark.parametrize(
        "attr_name",
        [
            'config.sources[9]',
        ]
    )
    def test_index_error(self, config_manager_object_valid, attr_name):
        config_manager = config_manager_object_valid
        with pytest.raises(IndexError):
            _ = eval(f'config_manager.{attr_name}')