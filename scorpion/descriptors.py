from src.utils import get_values_to_key_from_list_of_dict, items_unique_in_container


class SetValueOnlyOnceDescriptor:
    def __init__(self):
        self._value = None
        self._value_set = False

    def __get__(self, instance, owner):
        return self._value if self._value_set else None

    def __set__(self, instance, value):
        if self._value_set:
            raise AttributeError(f'Attribute "{self._name}" in "{instance.__class__.__name__}" can only be set once')
        self._value_set = True
        self._validate(value)
        self._value = value

    def __set_name__(self, owner, name):
        self._name = name

    def _validate(self, value):
        pass


class OutputConfigurationDescriptor(SetValueOnlyOnceDescriptor):

    _allowed_formats = ['excel', 'csv']

    def _validate(self, value):
        self._validate_target_format(value['target_format'])
        all_sheet_names = get_values_to_key_from_list_of_dict(value['output_tables'], 'output_table_name')
        items_unique_in_container(all_sheet_names, ValueError, 'sheets in output configuration')

    def _validate_target_format(self, target_format):
        if target_format not in self._allowed_formats:
            raise ValueError(f'Target format "{target_format}" is not supported')
