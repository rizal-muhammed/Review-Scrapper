from reviewScrapper.config import ConfigurationManager
from reviewScrapper.components import DataScrapper
from reviewScrapper import logger

STAGE_NAME = "Data Ingestion stage"


def main():
    config = ConfigurationManager()
    data_scrapper_config = config.get_data_scrapper_config()
    data_scrapper = DataScrapper(config=data_scrapper_config)
    data_scrapper.review_scrapper()


if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e
    