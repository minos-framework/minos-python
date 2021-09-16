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

    def test_queries_service(self):
        config = MinosConfig(path=self.config_file_path, queries_service="test")
        query = config.queries
        self.assertEqual("test", query.service)

    def test_commands_service(self):
        config = MinosConfig(path=self.config_file_path, commands_service="test")
        commands = config.commands
        self.assertEqual("test", commands.service)

    def test_events_service(self):
        config = MinosConfig(path=self.config_file_path, events_service="test")
        events = config.events
        self.assertEqual("test", events.service)

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
