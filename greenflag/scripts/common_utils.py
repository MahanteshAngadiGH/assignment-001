import logging
from config import config
#import datetime


def get_logger(name):
    """
    Create the logger object and pass it thoughout the etl job
    :param name:
    :return:
    """
    logger = logging.getLogger(name)
    logger.setLevel(config['log_level'])
    logger.info("Created the logger.")
    return logger
