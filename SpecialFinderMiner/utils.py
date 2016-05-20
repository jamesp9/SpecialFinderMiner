from config import config
import logging
import sys

def config_logger(logger):
    # Set logging level
    logger.setLevel(config.log_level)

    formatter = logging.Formatter('%(name)s: %(levelname)s %(asctime)s %(processName)s %(process)d %(funcName)s %(message)s')
    # Streamhandler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
