from configparser import ConfigParser
from typing import Callable, Union, List, Iterable
import os
import sys


class Config(object):
    """
    Configuration Object to read and process configuration file
    """
    def __init__(self: Callable, config_path: Union[str, List[str]]):
        """
        Initializes the Config class by reading the configuration using
        the given path

        Args:
            self (Config): Un-Initialized Config Class
            config_path (str): The Path of the Configuration file

        Returns:
            Config: Initialized Config class
        """

        self.config_path = config_path
        self.parse()

    def parse(self: Callable) -> Callable:
        """
        To Parse the given configuration file

        Args:
            self (Callable): The Un-Initialized Config Class

        Returns:
            Callable: Initialized Config Class
        """

        try:
            if not os.path.exists(self.config_path):
                raise Exception(f"The given Path {self.config_path} does not Exist")

            self.parser = ConfigParser(default_section='default')
            self.parser.read(self.config_path)
        except Exception as e:
            print("Exception occured", e)
            self.parser = None
            raise e

    def __getitem__(self: Callable, config_name: str) -> str:
        """
        The Method to get a configuration option

        Args:
            self (Config): Initialized Config Class
            config_name (str): The name of configuration to read

        Returns:
            str: Returns the Configuration as a String
        """

        try:
            config_names = config_name.split('.')
            return self.parser.get(*config_names)
        except Exception as e:
            print(f"Unable to read the configuration for name {config_name}.")
            print(f"Exception Occured, {e}")
            raise e

    def __contains__(self: Callable, name: str) -> bool:
        """To check whether the name present in the configuration or not

        Args:
            self (Callable): Initialized Config Class
            name (str): Name of configuration property to check

        Returns:
            bool: Whether the given name present in configuration or not
        """

        try:
            self[name]
            return True
        except Exception:
            return False

    def __iter__(self: Callable) -> Iterable[str]:
        """
        To Iterate through the Configuration

        Args:
            self (Callable): Initialized Config Class
        """
        if self.parser:
            for section in self.parser.sections():
                for name, value in self.parser.items(section):
                    yield section + '.' + name
        else:
            return None

    def __str__(self: Callable) -> str:
        """
        To print the Configuration as a string

        Args:
            self (Config): Initialized Config Class

        Returns:
            str: Returns the configuration as a string
        """

        if self.parser:
            res = '\n'
            for section in self.parser.sections():
                res += '[' + section + ']' + '\n'
                for name, value in self.parser.items(section):
                    res += name + '=' + value + '\n'
                res += '\n'
            return res
        else:
            return 'No Configuration Found.'


if __name__ == '__main__':
    """
    The below code will get execut when the file is ran using python 
    and doesn't get execute when imported.
    """
    if len(sys.argv) >= 2:
        config = Config(sys.argv[1])
        print(config)
        if len(sys.argv) >= 3:
            for i in range(2, len(sys.argv)):
                print(config[sys.argv[i]])
        for item in config:
            print(item, config[item])
    else:
        print("Please provide the configuration path")
