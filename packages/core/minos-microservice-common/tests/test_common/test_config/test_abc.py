import unittest
import warnings

from minos.common import (
    Config,
    ConfigV1,
    InjectableMixin,
    MinosConfig,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestConfig(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(Config, InjectableMixin))

    def test_get_injectable_name(self):
        self.assertTrue("config", ConfigV1.get_injectable_name())


class TestMinosConfig(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(MinosConfig, Config))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            config = MinosConfig(CONFIG_FILE_PATH)
            self.assertIsInstance(config, Config)


if __name__ == "__main__":
    unittest.main()
