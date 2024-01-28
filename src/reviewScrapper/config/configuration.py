from pathlib import Path

from reviewScrapper.constants import CONFIG_FILE_PATH, PARAMS_FILE_PATH
from reviewScrapper.utils import read_yaml, create_directories
from reviewScrapper.entity import (DataScrapperConfig)


class ConfigurationManager:
    def __init__(self,
                 config_filepath=CONFIG_FILE_PATH,
                 params_filepath=PARAMS_FILE_PATH):
        self.config = read_yaml(config_filepath)
        self.params = read_yaml(params_filepath)
        create_directories([self.config.artifacts_root])

    def get_data_scrapper_config(self) -> DataScrapperConfig:
        config = self.config.data_scrapper
        
        create_directories([config.root_dir])

        data_scrapper_config = DataScrapperConfig(
            root_dir=Path(config.root_dir),
            source_URL=str(config.source_URL),
            data_dir=Path(config.data_dir),
            meta_data_dir=Path(config.meta_data_dir),
            search_string=str(self.params.SEARCH_STRING)
        )

        return data_scrapper_config
    