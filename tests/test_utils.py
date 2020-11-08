import pytest

import scorpion.utils

from fixtures.fixtures import config_from_json, config_from_yaml, config_from_yaml_skip_first_level

@pytest.mark.skip
class TestConfigPositive:
    td_content_2nd_level = (
        "key, expected",
        [
            ('global', True),
            ('sources', True),
            ('process-steps', True),
            ('output', True),
        ]
    )

    @pytest.mark.parametrize(*td_content_2nd_level)
    def test_load_json_2nd_level(self, config_from_json, key, expected):
        assert (key in config_from_json['config']) == expected

    @pytest.mark.parametrize(*td_content_2nd_level)
    def test_load_yaml_2nd_level(self, config_from_yaml, key, expected):
        assert (key in config_from_yaml['config']) == expected

    def test_load_json_1st_level(self, config_from_json):
        assert ('config' in config_from_json) == True

    def test_load_yaml_1st_level(self, config_from_yaml):
        assert ('config' in config_from_yaml) == True

    def test_load_config_skip_1st_level(self, config_from_yaml_skip_first_level):
        assert ('global' in config_from_yaml_skip_first_level) == True

@pytest.mark.skip
class TestConfigNegative:

    def test_raises_unsupported_format(self, config_from_json):
        with pytest.raises(ValueError):
            path = 'fixtures/data/empty_dummy'
            _ = scorpion.utils.load_config(path, format_='xml')

    def test_raises_type_error(self):
        with pytest.raises(TypeError):
            path = 'fixtures/data/config_file.yaml'
            _ = scorpion.utils.load_config(path, format_='yaml', skip_first_level=True)

@pytest.mark.skip
class TestTransformToValidAttrName:
    td_transform_to_valid_attr_name = (
        "s, expected",
        [
            ('', ''),
            (' ', ''),
            ('           ', ''),
            ('4', '_'),
            ('47', '_7'),
            ('.5a3', '_5a3'),
            ('ßqÜ', '_qÜ'),
            ('-ßqÜ', '_ßqÜ'),
            ('.zu&ßx%Ä5a3', '_zu_ßx_Ä5a3'),
            (' a ', 'a'),
            ('def', 'def_'),
            ('_def', '_def'),
            ('eval', 'eval'),
            ('eval_', 'eval_'),
            ('key-with-dash', 'key_with_dash'),
            ('-', '_'),
            ('_-_', '___'),
            ('___', '___'),
            ('key with whitespace', 'key_with_whitespace'),
            (' key with  whitespace ', 'key_with__whitespace'),
            ('key\twith\twhitespace', 'key_with_whitespace'),
        ]
    )

    @pytest.mark.parametrize(*td_transform_to_valid_attr_name)
    def test_produce_attr_name(self, s, expected):
        assert scorpion.utils.transform_to_valid_attr_name(s) == expected


td_analyze_container_relationship = (
    "left, right, return_from_analyze_container_relationship, data_relationship_report",
    [
        (
            [],
            [],
            {
                'left': [],
                'right': [],
                'shared': [],
                'only_left': [],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': True,
                'right_is_subset_of_left': True,
            },
        ),
        (
            [None],
            [None],
            {
                'left': [None],
                'right': [None],
                'shared': [None],
                'only_left': [],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': True,
                'right_is_subset_of_left': True,
            },
        ),
        (
            [],
            [5],
            {
                'left': [],
                'right': [5],
                'shared': [],
                'only_left': [],
                'only_right': [5],
            },
            {
                'left_is_subset_of_right': True,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ['ab'],
            [],
            {
                'left': ['ab'],
                'right': [],
                'shared': [],
                'only_left': ['ab'],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': True,
            },
        ),
        (
            ['ab'],
            [5],
            {
                'left': ['ab'],
                'right': [5],
                'shared': [],
                'only_left': ['ab'],
                'only_right': [5],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ['ab', 5],
            [5],
            {
                'left': ['ab', 5],
                'right': [5],
                'shared': [5],
                'only_left': ['ab'],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': True,
            },
        ),
        (
            ['ab', 5],
            [5, 100, -9],
            {
                'left': ['ab', 5],
                'right': [5, 100, -9],
                'shared': [5],
                'only_left': ['ab'],
                'only_right': [100, -9],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ['ab', -9, 5],
            [5, 100, -9],
            {
                'left': ['ab', -9, 5],
                'right': [5, 100, -9],
                'shared': [-9, 5],
                'only_left': ['ab'],
                'only_right': [100],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ['ab', [60, True, None], -9, 5],
            [5, 100, -9, [60, True, None]],
            {
                'left': ['ab', [60, True, None], -9, 5],
                'right': [5, 100, -9, [60, True, None]],
                'shared': [[60, True, None], -9, 5],
                'only_left': ['ab'],
                'only_right': [100],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ['ab', [60, True, None], -9, 5],
            [5, 100, -9, [60, None, True]],
            {
                'left': ['ab', [60, True, None], -9, 5],
                'right': [5, 100, -9, [60, None, True]],
                'shared': [-9, 5],
                'only_left': ['ab', [60, True, None]],
                'only_right': [100, [60, None, True]],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ['ab', [60, True, None], -9, 5, 100],
            [5, 100, 'ab', -9, [60, True, None]],
            {
                'left': ['ab', [60, True, None], -9, 5, 100],
                'right': [5, 100, 'ab', -9, [60, True, None]],
                'shared': ['ab', [60, True, None], -9, 5, 100],
                'only_left': [],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': True,
                'right_is_subset_of_left': True,
            },
        ),
        (
            ['ab', [60, [True, 'hhh'], None], -9, 5, 100],
            [5, 100, 'ab', -9, [60, [True, 'hhh'], None]],
            {
                'left': ['ab', [60, [True, 'hhh'], None], -9, 5, 100],
                'right': [5, 100, 'ab', -9, [60, [True, 'hhh'], None]],
                'shared': ['ab', [60, [True, 'hhh'], None], -9, 5, 100],
                'only_left': [],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': True,
                'right_is_subset_of_left': True,
            },
        ),
        (
            ['ab', [60, [True, 'hhh'], None], -9, 5, 100],
            [5, 100, 'ab', -9, [60, ['hhh', True], None]],
            {
                'left': ['ab', [60, [True, 'hhh'], None], -9, 5, 100],
                'right': [5, 100, 'ab', -9, [60, ['hhh', True], None]],
                'shared': ['ab', -9, 5, 100],
                'only_left': [[60, [True, 'hhh'], None]],
                'only_right': [[60, ['hhh', True], None]],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ('ab', (60, (True, 'hhh'), None), -9, 5, 100),
            (5, 100, 'ab', -9, (60, ('hhh', True), None)),
            {
                'left': ('ab', (60, (True, 'hhh'), None), -9, 5, 100),
                'right': (5, 100, 'ab', -9, (60, ('hhh', True), None)),
                'shared': ['ab', -9, 5, 100],
                'only_left': [(60, (True, 'hhh'), None)],
                'only_right': [(60, ('hhh', True), None)],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': False,
            },
        ),
        (
            ('ab', (60, ('hhh', True), None), -9, 5, 100),
            (5, 100, 'ab', -9, (60, ('hhh', True), None)),
            {
                'left': ('ab', (60, ('hhh', True), None), -9, 5, 100),
                'right': (5, 100, 'ab', -9, (60, ('hhh', True), None)),
                'shared': ['ab', (60, ('hhh', True), None), -9, 5, 100],
                'only_left': [],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': True,
                'right_is_subset_of_left': True,
            },
        ),
        (
            ('ab', (60, ('hhh', True), None), 4j, -9, 5, 100),
            (5, 100, 'ab', -9, (60, ('hhh', True), None)),
            {
                'left': ('ab', (60, ('hhh', True), None), 4j, -9, 5, 100),
                'right': (5, 100, 'ab', -9, (60, ('hhh', True), None)),
                'shared': ['ab', (60, ('hhh', True), None), -9, 5, 100],
                'only_left': [4j],
                'only_right': [],
            },
            {
                'left_is_subset_of_right': False,
                'right_is_subset_of_left': True,
            },
        ),
        (
            ('ab', (60, ('hhh', True), None), -9, 5, 100),
            (5, 100, 'ab', -9, 7.77, (60, ('hhh', True), None)),
            {
                'left': ('ab', (60, ('hhh', True), None), -9, 5, 100),
                'right': (5, 100, 'ab', -9, 7.77, (60, ('hhh', True), None)),
                'shared': ['ab', (60, ('hhh', True), None), -9, 5, 100],
                'only_left': [],
                'only_right': [7.77],
            },
            {
                'left_is_subset_of_right': True,
                'right_is_subset_of_left': False,
            },
        ),
    ]
)

@pytest.mark.skip
@pytest.mark.parametrize(*td_analyze_container_relationship)
def test_analyze_container_relationship(
        left,
        right,
        return_from_analyze_container_relationship,
        data_relationship_report,
):
    return_from_analyze_container_relationship = \
        scorpion.utils._ContainerRelationshipReport(**return_from_analyze_container_relationship)

    assert scorpion.utils.analyze_container_relationship(left, right) == \
           return_from_analyze_container_relationship

    assert getattr(return_from_analyze_container_relationship, 'left_is_subset_of_right') == \
           data_relationship_report['left_is_subset_of_right']

    assert getattr(return_from_analyze_container_relationship, 'right_is_subset_of_left') == \
           data_relationship_report['right_is_subset_of_left']


td_filter_mapping_positive = (
    'mapping, filter_keys, method, silent_key_error, expected',
    [
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            ['one_key', 'third_key'],
            'keep',
            True,
            {
                'one_key': 15,
                'third_key': [i for i in range(10)],
            },
        ),
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            [],
            'keep',
            True,
            {},
        ),
        (
            {},
            [],
            'keep',
            True,
            {},
        ),
        (
            {},
            [],
            'drop',
            True,
            {},
        ),
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            ['one_key', 'third_key', 'extra_key'],
            'drop',
            True,
            {
                'other_key': True,
            },
        ),
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            [],
            'drop',
            True,
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],
            },
        ),
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            [],
            'drop',
            False,
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],
            },
        ),
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            [],
            'keep',
            False,
            {},
        ),
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            ['one_key'],
            'drop',
            False,
            {
                'other_key': True,
                'third_key': [i for i in range(10)],
            },
        ),
    ],
)

@pytest.mark.skip
@pytest.mark.parametrize(*td_filter_mapping_positive)
def test_filter_keys_positive(mapping, filter_keys, method, silent_key_error, expected):
    assert scorpion.utils.filter_mapping(mapping, filter_keys, method, silent_key_error) == expected


td_filter_mapping_key_error = (
    'mapping, filter_keys, method, silent_key_error, expected',
    [
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            ['one_key', 'third_key', 'extra_key'],
            'keep',
            False,
            {},
        ),
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            ['one_key', 'third_key', 'extra_key'],
            'drop',
            False,
            {},
        ),
    ],
)

@pytest.mark.skip
@pytest.mark.parametrize(*td_filter_mapping_key_error)
def test_filter_keys_key_error(mapping, filter_keys, method, silent_key_error, expected):
    with pytest.raises(KeyError):
        _ = scorpion.utils.filter_mapping(mapping, filter_keys, method, silent_key_error)


td_filter_mapping_value_error = (
    'mapping, filter_keys, method, silent_key_error, expected',
    [
        (
            {
                'one_key': 15,
                'other_key': True,
                'third_key': [i for i in range(10)],

            },
            ['one_key', 'third_key', 'extra_key'],
            'other_mode',
            False,
            {},
        ),
    ],
)

@pytest.mark.skip
@pytest.mark.parametrize(*td_filter_mapping_value_error)
def test_filter_keys_value_error(mapping, filter_keys, method, silent_key_error, expected):
    with pytest.raises(ValueError):
        _ = scorpion.utils.filter_mapping(mapping, filter_keys, method, silent_key_error)


td_rename_keys = (
    'mapping, keys, keep_keys_old, silent_key_error, expected',
    [
        (
            {
                'a': True,
                'b': 44,
                'c': 'string',
            },
            ('a', 'aa'),
            False,
            True,
            {
                'aa': True,
                'b': 44,
                'c': 'string',
            },
        ),
        (
            {
                'a': True,
                'b': 44,
                'c': 'string',
            },
            [('a', 'aa'), ('c', '1234')],
            False,
            True,
            {
                'aa': True,
                'b': 44,
                '1234': 'string',
            },
        ),
        (
            {
                'a': True,
                'b': 44,
                'c': 'string',
            },
            [('a', 'aa'), ('c', '1234')],
            True,
            False,
            {
                'a': True,
                'b': 44,
                'c': 'string',
                'aa': True,
                '1234': 'string',
            },
        ),
    ],
)


@pytest.mark.parametrize(*td_rename_keys)
def test_filter_rename_keys_positive(mapping, keys, keep_keys_old, silent_key_error, expected):
    assert scorpion.utils.rename_keys(mapping, keys, keep_keys_old, silent_key_error) == expected


td_rename_keys_key_error_missing_key = (
    'mapping, keys, keep_keys_old, silent_key_error, expected',
    [
        (
            {
                'a': True,
                'b': 44,
                'c': 'string',
            },
            ('key_not_in', 'aa'),
            False,
            False,
            {},
        ),

    ],
)


@pytest.mark.parametrize(*td_rename_keys_key_error_missing_key)
def test_rename_keys_key_error_missing_key(mapping, keys, keep_keys_old, silent_key_error, expected):
    with pytest.raises(KeyError):
        _ = scorpion.utils.rename_keys(mapping, keys, keep_keys_old, silent_key_error)


td_rename_keys_unsafe_keys_new = (
    'mapping, keys, keep_keys_old, silent_key_error, expected',
    [
        (
            {
                'a': True,
                'b': 44,
                'c': 'string',
            },
            ('key_not_in', 'aa'),
            False,
            False,
            {},
        ),

    ],
)


@pytest.mark.parametrize(*td_rename_keys_unsafe_keys_new)
def test_rename_keys_unsafe_keys_new(mapping, keys, keep_keys_old, silent_key_error, expected):
    with pytest.raises(KeyError):
        _ = scorpion.utils.rename_keys(mapping, keys, keep_keys_old, silent_key_error)