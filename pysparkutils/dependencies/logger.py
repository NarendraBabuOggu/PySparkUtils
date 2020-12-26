from logging import Logger
import logging


def get_logger(loglevel: str='ERROR') -> Logger:
    """
    Create Logger Object for Logging the messages

    Args:
        loglevel (str): Log Level to use ['DEBUG', 'INFO', 'WARN', 'ERROR']
    
    Returns:
        Returns the Logger object to log the messages
    """

    logger = logging.getLogger('PySpark Utils')
    logger.setLevel(loglevel)
    s_handler = logging.StreamHandler()

    # Using format similar to Spark Log format
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s: %(message)s', datefmt='%y/%m/%d %H:%M:%S')
    s_handler.setFormatter(formatter)
    logger.addHandler(s_handler)

    return logger
