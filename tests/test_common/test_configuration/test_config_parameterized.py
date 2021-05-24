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


class MyTestCase(unittest.TestCase):
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
