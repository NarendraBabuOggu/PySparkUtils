#from pysparkutils.dependencies.config_parser import Config
from pysparkutils.dependencies.hocon_config_parser import get_config
import unittest
from typing import Callable
from pyhocon.config_tree import ConfigTree


class TestConfig(unittest.TestCase):

    def test_existing_file(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        config = get_config('tests/configs/hocon.conf')
        self.assertTrue(
            config,
            "Configuration File tests/configs/hocon.conf but Config Class" + 
            "Failed to Read"
        )
        self.assertIsInstance(
            config,
            ConfigTree,
            "Configuration File should be of type ConfigTree class." + 
            f"Where as the result is of type {type(config)}"
        )

    def test_non_existing_file(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        with self.assertRaises(Exception):
            get_config('config.ini')

    def test_existing_property(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        config = get_config('tests/configs/hocon.conf')
        self.assertEqual(
            config['test.spark.master'],
            'local[2]'
        )

    def test_non_existing_property(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        config = get_config('tests/configs/hocon.conf')
        with self.assertRaises(Exception):
            self.assertNotEqual(
                config['dev.master'],
                'local'
            )


if __name__ == '__main__':
    unittest.main()
