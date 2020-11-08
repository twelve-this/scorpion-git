import pandas as pd
import datetime
import os

from src.utils import AutoReprMixin, singleton

from src.descriptors import SetValueOnlyOnceDescriptor, OutputConfigurationDescriptor


class OutputManagerNotReadyError(Exception):
    pass


@singleton
class OutputManager(AutoReprMixin):

    global_configuration = SetValueOnlyOnceDescriptor()
    output_configuration = OutputConfigurationDescriptor()
    data_frames = SetValueOnlyOnceDescriptor()

    def _is_ready(self):

        required_attributes = [
            self.global_configuration,
            self.output_configuration,
            self.data_frames
        ]
        try:
            if None in required_attributes:
                raise OutputManagerNotReadyError(
                    f'Output Manager is not ready; not all required attributes are provided: {required_attributes}'
                )

            for output_table in self.output_configuration['output_tables']:
                if not output_table['skip']:
                    try:
                        _ = output_table['output_table_data_frame'] in self.data_frames
                    except KeyError as err:
                        raise OutputManagerNotReadyError(
                            f""""{output_table['output_table_data_frame']}" is required for output production but was not supplied by DFManager""") from err
        except (OutputManagerNotReadyError, KeyError):
            raise
        else:
            return True

    def produce_output(self):

        if self._is_ready():

            if not self.output_configuration['skip']:
                current_date_file_name = self._get_current_date()
                if self.output_configuration['target_format'] == 'excel':
                    self._produce_excel_output(current_date=current_date_file_name)
                elif self.output_configuration['target_format'] == 'csv':
                    self._produce_csv_output(current_date=current_date_file_name)
                print('Production of output is complete')
            else:
                print(f'Production of output is skipped')

    def _get_current_date(self):
        current_date = ''
        if self.output_configuration['current_date_suffix_to_target_file_name']:
            date_format = self.global_configuration.get_config_item('date_format').value
            current_date = f'{datetime.datetime.now().strftime(date_format)}'
        return current_date

    def _produce_excel_output(self, current_date):
        target_file_name = self._produce_target_file_name(current_date=current_date)
        with pd.ExcelWriter(target_file_name) as writer:
            for output_table in self.output_configuration['output_tables']:
                if not output_table['skip']:
                    df = self.data_frames.get_data_frame(output_table['output_table_data_frame'])
                    sheet_name = output_table['output_table_name']
                    columns = output_table['output_table_columns'] if len(output_table['output_table_columns']) > 0 else None
                    df.to_excel(writer, sheet_name=sheet_name, columns=columns)
                else:
                    print(f'Output for table "{output_table["output_table_name"]}" was skipped')

    def _produce_csv_output(self, current_date):
        for output_table in self.output_configuration['output_tables']:
            if not output_table['skip']:
                file_name = self._produce_target_file_name(csv_part=output_table['output_table_name'], current_date=current_date)
                df = self.data_frames.get_data_frame(output_table['output_table_data_frame'])
                columns = output_table['output_table_columns'] if len(output_table['output_table_columns']) > 0 else None
                df.to_csv(path_or_buf=file_name, encoding='UTF-8', sep=';', columns=columns)
            else:
                print(f'Output for table "{output_table["output_table_name"]}" was skipped')

    def _produce_target_file_name(self, csv_part=None, current_date=None):

        current_date = current_date if current_date else ''

        target_folder = f"{self.output_configuration['target_folder']}__{current_date}"
        if not os.path.exists(target_folder):
            os.mkdir(target_folder)

        supported_output_formats = {
            'csv': 'csv',
            'excel': 'xlsx',
        }

        target_file_name = self.output_configuration['target_file_name']
        if '.' in target_file_name:
            target_file_name = target_file_name.partition('.')[0]

        csv_part = f'__{csv_part}' if csv_part is not None else ''

        file_extension = supported_output_formats[self.output_configuration['target_format']]

        file_name = f'{target_file_name}{csv_part}__{current_date}.{file_extension}'
        full_path = os.path.join(target_folder, file_name)
        return full_path
