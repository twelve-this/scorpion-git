import pandas as pd
from dataclasses import dataclass
from typing import List, Set, Dict, Union

from src.utils import AutoReprMixin, DFManagerMixin, singleton


class DataProcessorManagerError(Exception):
    pass


@dataclass
class ProcessInstruction:
    uses_data_processor: str
    step: int
    skip: bool
    description: str
    uses_data_frames_for_input: List[str]
    expected_output_data_frames: List[str]

    def __getattr__(self, item):
        return self.__dict__[item]

    @property
    def header(self) -> List[str]:
        return list(self.__dict__.keys())


class ProcessInstructionContainer:

    def __init__(self) -> None:
        self.process_instructions = None

    def __get__(self, instance, owner) -> \
            Union['ProcessInstructionContainer', List[ProcessInstruction]]:
        if instance is None:
            return self
        return self.process_instructions

    def __set__(self, instance, process_instructions) -> Union['ProcessInstructionContainer', None]:
        if instance is None:
            return self
        if self.process_instructions is not None:
            raise AttributeError('Process instructions can only be set once')
        if len(process_instructions) == 0:
            self.process_instructions = []
        else:
            self.validate(process_instructions)
            self.process_instructions = self.create_process_instructions(process_instructions)
            self.process_instructions = self.sort(self.process_instructions)

    def __set_name__(self, owner, name) -> None:
        self.name = name

    def validate(self, process_instructions) -> None:
        self.check_steps(process_instructions)

    def check_steps(self, process_instructions) -> None:
        steps = []
        for process_instruction in process_instructions:
            step = process_instruction['step']
            if not isinstance(step, int):
                raise TypeError(
                    f'Attribute step must be of int type; this was given: "{step}" which is of "{type(step)}" type\n'
                    f'Given process instruction: {process_instruction}'
                )
            if step in steps:
                raise ValueError(
                    f'Step {step} - which is in "{process_instruction}" - is not unique'
                )
            steps.append(step)

    def create_process_instructions(self, process_instructions) -> List[ProcessInstruction]:

        return [ProcessInstruction(
            uses_data_processor=process_instruction['uses_data_processor'],
            step=process_instruction['step'],
            skip=process_instruction['skip'],
            description=process_instruction['description'],
            uses_data_frames_for_input=
            process_instruction['uses_data_frames_for_input'],
            expected_output_data_frames=process_instruction['expected_output_data_frames'])

            for process_instruction in process_instructions]

    def sort(self, process_instructions) -> List[ProcessInstruction]:
        return sorted(process_instructions, key=lambda x: x.step)


class DataProcessorContainer:

    def __init__(self) -> None:
        self.data_processors = None

    def __get__(self, instance, owner) -> \
            Union['DataProcessorContainer', Dict[str, 'DataProcessor']]:
        if instance is None:
            return self
        return self.data_processors

    def __set__(self, instance, data_processors):
        if instance is None:
            return self
        if self.data_processors is not None:
            raise AttributeError('Processors can only be set once')
        if len(data_processors) == 0:
            self.data_processors = {}
        else:
            self.validate(data_processors)
            self.data_processors = self.create_mapping(data_processors)

    def __set_name__(self, owner, name):
        self.name = f'{owner.__name__}.{name}'

    def create_mapping(self, data_processors):
        return {data_processor.key: data_processor for data_processor in data_processors}

    def validate(self, data_processors):
        self.keys_unique(data_processors)

    def keys_unique(self, data_processors):
        keys = set()
        for data_processor in data_processors:
            key = data_processor.key
            if key in keys:
                raise KeyError(f'Key "{key}" in {self.name} is not unique; these are all known keys: {keys}')
            else:
                keys.add(key)


@singleton
class DataProcessorManager(AutoReprMixin, DFManagerMixin):
    process_instructions = ProcessInstructionContainer()
    data_processors = DataProcessorContainer()

    def __init__(self):
        super().__init__()

    @property
    def process_instructions_not_skipped(self) -> List[ProcessInstruction]:
        return [process_instruction for process_instruction in self.process_instructions
                if not process_instruction.skip]

    def get_data_processor_by_key(self, key) -> 'DataProcessor':
        return self.data_processors[key]

    @property
    def available_data_processors(self) -> List[str]:
        return [key for key in self.data_processors]

    @property
    def required_data_processors(self) -> Set[str]:
        return {process_instruction.uses_data_processor for process_instruction in self.process_instructions}

    def process(self, skip=False) -> None:
        if not skip:
            self._data_processors_available()
            self._exec_process_instructions()
        else:
            print('Data processing is skipped')

    def _check_processing_readiness(self) -> None:
        self._processors_available()

    def _data_processors_available(self) -> None:
        for required_data_processor in self.required_data_processors:
            if required_data_processor not in self.available_data_processors:
                raise DataProcessorManagerError(
                    f'Processor for "{required_data_processor}" not available'
                )

    def _exec_process_instructions(self) -> None:
        for process_instruction in self.process_instructions_not_skipped:
            processor = self.get_data_processor_by_key(process_instruction.uses_data_processor)()
            df_input = self._df_manager.get_data_frames(process_instruction.uses_data_frames_for_input)
            processor.add_input_data_frames(df_input)
            processor.set_expected_output_data_frames(process_instruction.expected_output_data_frames)
            processor.process()
            # processor_output = self._receive_processor_output(process_instruction.returns_data_frames, processor.output)
            self._df_manager.add_data_frames(processor.output)

    # def _receive_processor_output(self, df_output_names: List[str], output: Dict[str, pd.DataFrame]):
    #     message = f'Mismatch in {inspect.currentframe().f_code.co_name}'
    #     one_in_the_other_and_vc(df_output_names, output.keys(), DataProcessorManagerError, message)
    #     return output

    @property
    def readout(self):  # Ugly code, ugly hack
        if None in [self.process_instructions, self.data_processors]:
            raise DataProcessorManagerError(
                'Process instruction readout is not available because process instructions are not set')
        else:
            data = None
            for process_instruction in self.process_instructions:
                if data is None:
                    data = {head: [] for head in process_instruction.header}
                for head, value in data.items():
                    value.append(process_instruction.__getattr__(head))

            df = pd.DataFrame(data=data)
            order_columns = [
                'step',
                'skip',
                'description',
                'uses_data_processor',
                'uses_data_frames_for_input',
                'expected_output_data_frames',
            ]
            df = df.reindex(columns=order_columns)
            with pd.option_context('display.max_rows', None,
                                   'display.max_columns', None,
                                   'display.max_colwidth', None,
                                   'display.expand_frame_repr', False):
                print('*' * 200)
                print('These process instructions will be applied:')
                print(df)
                print('*' * 200)
                print()
