import os
import unittest
from unittest import (
    mock,
)

from minos.common import (
    ConfigV2,
)
from tests.utils import (
    BASE_PATH,
)


class TestConfigV2Parameterized(unittest.TestCase):
    def setUp(self) -> None:
        self.file_path = BASE_PATH / "config" / "v2.yml"

    def test_overwrite_with_parameter(self):
        config = ConfigV2(self.file_path, databases_default_database="foo")
        database_config = config.get_default_database()
        self.assertEqual("foo", database_config["database"])

    @mock.patch.dict(os.environ, {"MINOS_DATABASES_DEFAULT_DATABASE": "foo"})
    def test_overwrite_with_parameter_priority(self):
        config = ConfigV2(self.file_path, databases_default_database="bar")
        repository = config.get_default_database()
        self.assertEqual("bar", repository["database"])


if __name__ == "__main__":
    unittest.main()
