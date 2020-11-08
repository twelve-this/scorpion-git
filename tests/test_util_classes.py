import pytest

from fixtures.fixtures import generic_manager


class TestAbstractManagerUsingDataFrameManagerPositive:

    data = {
        'level0_0': {
            'level1_0': {
                'value1_0_0': True,
                'value1_0_1': 'random_string'

            }
        },
        'level0_1': {

        }
    }

    def test___iter__(self):
        print(m)

    def test___len__(self):
        pass

    def test___get_item__(self):
        pass

    def test___set_item__(self):
        pass

    def test___contains__(self):
        pass

    def test___eq__(self):
        pass

    def test___hash__(self):
        pass

    def test_get_multiple_items(self):
        pass

    def test_set_multiple_items(self):
        pass

    # content_2nd_level = (
    #     "key, expected",
    #     [
    #         ('global', True),
    #         ('sources', True),
    #         ('process-steps', True),
    #         ('output', True),
    #     ]
    # )
    #
    # @pytest.mark.parametrize(*content_2nd_level)
    # def test_load_json_2nd_level(self, config_from_json, key, expected):
    #     assert (key in config_from_json['config']) == expected