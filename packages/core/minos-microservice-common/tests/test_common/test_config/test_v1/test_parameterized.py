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


class TestConfigParameterized(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"

    def test_overwrite_with_parameter(self):
        config = ConfigV1(path=self.config_file_path, repository_database="foo")
        repository = config.repository
        self.assertEqual("foo", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_parameter_priority(self):
        config = ConfigV1(path=self.config_file_path, repository_database="bar")
        repository = config.repository
        self.assertEqual("bar", repository.database)

    def test_config_discovery(self):
        config = ConfigV1(
            path=self.config_file_path,
            minos_discovery_client="some-type",
            minos_discovery_host="some-host",
            minos_discovery_port=333,
        )
        discovery = config.discovery
        self.assertEqual("some-type", discovery.client)
        self.assertEqual("some-host", discovery.host)
        self.assertEqual(333, discovery.port)

    def test_config_service_injections_list(self):
        config = ConfigV1(path=self.config_file_path, service_injections=["foo", "bar"])
        self.assertEqual(["foo", "bar"], config.service.injections)

    def test_config_service_injections_dict(self):
        config = ConfigV1(path=self.config_file_path, service_injections={"one": "foo", "two": "bar"})
        self.assertEqual(["foo", "bar"], config.service.injections)


if __name__ == "__main__":
    unittest.main()
