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


class TestConfigV1Parameterized(unittest.TestCase):
    def setUp(self) -> None:
        self.file_path = BASE_PATH / "config" / "v1.yml"

    def test_overwrite_with_parameter(self):
        config = ConfigV1(self.file_path, repository_database="foo")
        database_config = config.get_default_database()
        self.assertEqual("foo", database_config["database"])

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_parameter_priority(self):
        config = ConfigV1(self.file_path, repository_database="bar")
        repository = config.get_default_database()
        self.assertEqual("bar", repository["database"])

    def test_config_discovery(self):
        config = ConfigV1(
            self.file_path,
            minos_discovery_client="builtins.int",
            minos_discovery_host="some-host",
            minos_discovery_port=333,
        )
        discovery = config.get_discovery()
        self.assertEqual(int, discovery["client"])
        self.assertEqual("some-host", discovery["host"])
        self.assertEqual(333, discovery["port"])

    def test_config_service_injections_list(self):
        config = ConfigV1(self.file_path, service_injections=["builtins.int", "builtins.float"])
        self.assertEqual([int, float], config.get_injections())

    def test_config_service_injections_dict(self):
        config = ConfigV1(self.file_path, service_injections={"one": "builtins.int", "two": "builtins.float"})
        self.assertEqual([int, float], config.get_injections())


if __name__ == "__main__":
    unittest.main()
