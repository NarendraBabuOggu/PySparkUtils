from pysparkutils.dependencies.config_parser import Config
import unittest
from typing import Callable


class TestConfig(unittest.TestCase):

    def test_existing_file(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        config = Config('tests/data/config.ini')
        self.assertTrue(
            config,
            "Configuration File data/config.ini Exists but Config Class" + 
            "Failed to Read"
        )
        self.assertIsInstance(
            config,
            Config,
            "Configuration File should be of type Config class." + 
            f"Where as the result is of type {type(config)}"
        )

    def test_non_existing_file(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        with self.assertRaises(Exception):
            Config('config.ini')

    def test_existing_property(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        config = Config('tests/data/config.ini')
        self.assertEqual(
            config['dev.master'],
            'local[*]'
        )

    def test_non_existing_property(self: Callable):
        """
        Method to test Config Class against existing file

        Args:
            self (Callable): Test case to validate Config Class
        """

        config = Config('tests/data/config.ini')
        self.assertNotEqual(
            config['dev.master'],
            'local'
        )


if __name__ == '__main__':
    unittest.main()
