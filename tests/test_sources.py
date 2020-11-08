import pytest

import scorpion.sources


class TestSourceConfigPositive:
    td_source_config_positive = (
        'source_name, default_priority, config_default, config_source, required_config_items_in_source, expected',
        [
            (
                'books',
                'default',
                {
                    'fillna': 'n/a',
                    'nrows': 100,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'default',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': 'n/a',
                    'nrows': 100,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'source',
                {
                    'fillna': 'n/a',
                    'nrows': 100,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'default',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': 'n/a',
                    'nrows': 100,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'source',
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'default',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'nrows': 100,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'source',
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'source',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': 'n/a',
                    'nrows': 111,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'default',
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'source',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'default',
                {
                    'fillna': 'n/a',
                    # 'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'source',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': 'n/a',
                    'nrows': 111,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'source',
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'source',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'fillna': None,
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': None,
                    'nrows': 111,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'default',
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'source',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'fillna': None,
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': 'n/a',
                    'nrows': 500,
                    'columns': ['column_1', 'column_2'],
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'default',
                {
                    # 'fillna': 'n/a',
                    # 'nrows': 500,
                    # 'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'source',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'fillna': None,
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': None,
                    'nrows': 111,
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'source',
                {
                    # 'fillna': 'n/a',
                    # 'nrows': 500,
                    # 'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'source',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'fillna': None,
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': None,
                    'nrows': 111,
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'source',
                {
                    # 'fillna': 'n/a',
                    # 'nrows': 500,
                    # 'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'default',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'fillna': None,
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': None,
                    'nrows': 111,
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
            (
                'books',
                'default',
                {
                    # 'fillna': 'n/a',
                    # 'nrows': 500,
                    # 'columns': ['column_1', 'column_2'],
                },
                {
                    'priority': 'default',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                    'fillna': None,
                    'nrows': 111,
                },
                ['priority', 'file_name', 'encoding'],
                {
                    'fillna': None,
                    'nrows': 111,
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
            ),
        ],
    )

    @pytest.mark.parametrize(*td_source_config_positive)
    def test_source_config_positive(self,
                                    source_name,
                                    default_priority,
                                    config_default,
                                    config_source,
                                    required_config_items_in_source,
                                    expected):
        source_config = scorpion.sources.SourceConfig(
            source_name,
            default_priority,
            config_default,
            config_source,
            required_config_items_in_source,
        )
        assert source_config.config == expected

    td_source_config_negative = (
        'source_name, default_priority, config_default, config_source, required_config_items_in_source, expected',
        [
            (
                'books',
                'default',
                {
                    'fillna': 'n/a',
                    'nrows': 100,
                    'columns': ['column_1', 'column_2'],
                },
                {
                    # 'priority': 'default',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
                ['priority', 'file_name', 'encoding'],
                {},
            ),
            (
                'books',
                'default',
                {
                    'fillna': 'n/a',
                    'nrows': 100,
                    'columns': ['column_1', 'column_2'],
                    'priority': 'source',
                },
                {
                    # 'priority': 'default',
                    'file_name': 'books.csv',
                    'encoding': 'UTF-8',
                },
                ['priority', 'file_name', 'encoding'],
                {},
            ),
        ],
    )

    @pytest.mark.parametrize(*td_source_config_negative)
    def test_source_config_negative(self,
                                    source_name,
                                    default_priority,
                                    config_default,
                                    config_source,
                                    required_config_items_in_source,
                                    expected):
        source_config = scorpion.sources.SourceConfig(
            source_name,
            default_priority,
            config_default,
            config_source,
            required_config_items_in_source,
        )
        with pytest.raises(scorpion.sources.SourceManagementError):
            _ = source_config.config
