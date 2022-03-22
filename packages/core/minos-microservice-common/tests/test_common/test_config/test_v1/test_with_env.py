import os
import unittest
from unittest import (
    mock,
)

from minos.common import (
    ConfigV1,
)
from tests.utils import (
    BASE_PATH,
)


class TestConfigWithEnvironment(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "config" / "v1.yml"
        self.config = ConfigV1(path=self.config_file_path)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment(self):
        repository = self.config.get_database_by_name()
        self.assertEqual("foo", repository["database"])

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment_false(self):
        self.config._with_environment = False
        repository = self.config.get_database_by_name()
        self.assertEqual("order_db", repository["database"])

    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_CLIENT": "builtins.int"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_HOST": "some-host"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_PORT": "333"})
    def test_config_discovery(self):
        discovery = self.config.get_discovery()
        self.assertEqual(int, discovery["client"])
        self.assertEqual("some-host", discovery["host"])
        self.assertEqual("333", discovery["port"])
