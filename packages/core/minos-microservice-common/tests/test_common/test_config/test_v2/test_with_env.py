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


class TestConfigV2WithEnvironment(unittest.TestCase):
    def setUp(self) -> None:
        self.file_path = BASE_PATH / "config" / "v2.yml"

    @mock.patch.dict(os.environ, {"MINOS_DATABASES_DEFAULT_DATABASE": "foo"})
    def test_overwrite_with_environment(self):
        database = ConfigV2(self.file_path).get_default_database()
        self.assertEqual("foo", database["database"])

    @mock.patch.dict(os.environ, {"MINOS_DATABASES_DEFAULT_DATABASE": "foo"})
    def test_overwrite_with_environment_false(self):
        database = ConfigV2(self.file_path, with_environment=False).get_default_database()
        self.assertEqual("order_db", database["database"])


if __name__ == "__main__":
    unittest.main()
