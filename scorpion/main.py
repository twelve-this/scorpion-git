from collections import OrderedDict

from scorpion.utils import load_config
from scorpion.config import Config
from scorpion.data_frame_manager import DataFrameManager
from scorpion.sources import SourceManager



def main() -> None:
    pass


if __name__ == '__main__':

    config_raw = load_config('config.yaml', 'yaml', skip_first_level=True, first_level_key='config')
    config = Config(config_raw)
    data_frame_manager = DataFrameManager()
    source_manager = SourceManager()
    source_manager.config = config
    sources = source_manager.prepare_sources()
    data_frame_manager.set_multiple_items(sources)
