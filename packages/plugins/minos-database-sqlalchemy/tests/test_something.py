import unittest

from minos.plugins.sqlalchemy import (
    __version__,
)


class TestSomething(unittest.TestCase):
    def test_version(self):
        self.assertIsInstance(__version__, str)


if __name__ == "__main__":
    unittest.main()
