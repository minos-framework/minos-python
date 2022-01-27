import os
import unittest
from unittest import (
    mock,
)

from minos.common import (
    MinosConfig,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosConfigWithEnvironment(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"
        self.config = MinosConfig(path=self.config_file_path)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment(self):
        repository = self.config.repository
        self.assertEqual("foo", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment_false(self):
        self.config._with_environment = False
        repository = self.config.repository
        self.assertEqual("order_db", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_CLIENT": "some-type"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_HOST": "some-host"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_PORT": "333"})
    def test_config_discovery(self):
        discovery = self.config.discovery
        self.assertEqual("some-type", discovery.client)
        self.assertEqual("some-host", discovery.host)
        self.assertEqual("333", discovery.port)
