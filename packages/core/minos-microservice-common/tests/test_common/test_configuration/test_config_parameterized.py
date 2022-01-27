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


class TestMinosConfigParameterized(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"

    def test_overwrite_with_parameter(self):
        config = MinosConfig(path=self.config_file_path, repository_database="foo")
        repository = config.repository
        self.assertEqual("foo", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_parameter_priority(self):
        config = MinosConfig(path=self.config_file_path, repository_database="bar")
        repository = config.repository
        self.assertEqual("bar", repository.database)

    def test_config_discovery(self):
        config = MinosConfig(
            path=self.config_file_path,
            minos_discovery_client="some-type",
            minos_discovery_host="some-host",
            minos_discovery_port=333,
        )
        discovery = config.discovery
        self.assertEqual("some-type", discovery.client)
        self.assertEqual("some-host", discovery.host)
        self.assertEqual(333, discovery.port)
