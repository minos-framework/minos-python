import unittest

from minos.common import (
    Config,
    DependencyInjector,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
    MinosTestCase,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestMinosTestCase(unittest.IsolatedAsyncioTestCase):
    def test_config(self):
        test_case = MyMinosTestCase()
        test_case.setUp()
        self.assertIsInstance(test_case.config, Config)

    def test_injector(self):
        test_case = MyMinosTestCase()
        test_case.setUp()
        self.assertIsInstance(test_case.injector, DependencyInjector)


class MyMinosTestCase(MinosTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH


class TestMyDatabaseMinosTestCase(unittest.IsolatedAsyncioTestCase):
    pass


class MyDatabaseMinosTestCase(DatabaseMinosTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH


if __name__ == "__main__":
    unittest.main()
