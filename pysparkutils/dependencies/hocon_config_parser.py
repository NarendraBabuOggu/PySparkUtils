from pyhocon import ConfigFactory, ConfigTree
import argparse
from pysparkutils.dependencies.logger import get_logger
from logging import Logger
from typing import Optional

def get_config(path: str, logger: Logger=None) -> Optional[ConfigTree]:
    """
    Function to read Configuration from a HOCON file

    Args:
        path (str): Path of Hocon File

    Returns:
        ConfigTree containing the configuration
    """
    try:
        if logger:
            logger.info(f"Reading Configuration from {path}")
        conf = ConfigFactory.parse_file(path)
        return conf
    except Exception as e:
        if logger:
            logger.warning(f"Exception occured while reading Configuration from {path}")
            logger.error(e)
        raise(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = "hocon_config_parser.py",
        usage = """python hocon_config_parser.py --path="path/to/hocon/config/file.conf" """,
        description = "Argument Parser for Hocon Config Parser",
        add_help = True
    )

    parser.add_argument(
        '-p', '--path', 
        default='configs/hocon.conf', 
        help="Path of the Hocon Config file", 
        dest='path'
    )

    args = parser.parse_args()

    # Defining Logger
    logger = get_logger('INFO')
    conf = get_config(args.path, logger)
    assert isinstance(conf, ConfigTree)
