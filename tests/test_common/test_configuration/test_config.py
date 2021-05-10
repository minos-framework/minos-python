"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import os
import unittest
from unittest import (
    mock,
)

from minos.common import (
    MinosConfig,
    MinosConfigAbstract,
    MinosConfigDefaultAlreadySetException,
    MinosConfigException,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosConfig(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"

    def test_config_ini_fail(self):
        with self.assertRaises(MinosConfigException):
            MinosConfig(path=BASE_PATH / "test_fail_config.yaml")

    def test_config_service(self):
        config = MinosConfig(path=self.config_file_path)
        service = config.service
        self.assertEqual("Order", service.name)

    def test_config_rest(self):
        config = MinosConfig(path=self.config_file_path)
        rest = config.rest

        broker = rest.broker
        self.assertEqual("localhost", broker.host)
        self.assertEqual(8900, broker.port)

        endpoints = rest.endpoints
        self.assertEqual("AddOrder", endpoints[0].name)

    def test_config_events(self):
        config = MinosConfig(path=self.config_file_path)
        events = config.events
        broker = events.broker
        self.assertEqual("localhost", broker.host)
        self.assertEqual(9092, broker.port)

    def test_config_events_queue_database(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        events = config.events
        queue = events.queue
        self.assertEqual("order_db", queue.database)
        self.assertEqual("minos", queue.user)
        self.assertEqual("min0s", queue.password)
        self.assertEqual("localhost", queue.host)
        self.assertEqual(5432, queue.port)
        self.assertEqual(10, queue.records)
        self.assertEqual(2, queue.retry)

    def test_config_commands_queue_database(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        commands = config.commands
        queue = commands.queue
        self.assertEqual("order_db", queue.database)
        self.assertEqual("minos", queue.user)
        self.assertEqual("min0s", queue.password)
        self.assertEqual("localhost", queue.host)
        self.assertEqual(5432, queue.port)
        self.assertEqual(10, queue.records)
        self.assertEqual(2, queue.retry)

    def test_config_saga_queue_database(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        saga = config.saga
        queue = saga.queue
        self.assertEqual("order_db", queue.database)
        self.assertEqual("minos", queue.user)
        self.assertEqual("min0s", queue.password)
        self.assertEqual("localhost", queue.host)
        self.assertEqual(5432, queue.port)
        self.assertEqual(10, queue.records)
        self.assertEqual(2, queue.retry)

    def test_config_repository(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        repository = config.repository
        self.assertEqual("order_db", repository.database)
        self.assertEqual("minos", repository.user)
        self.assertEqual("min0s", repository.password)
        self.assertEqual("localhost", repository.host)
        self.assertEqual(5432, repository.port)

    def test_config_snapshot(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        snapshot = config.snapshot
        self.assertEqual("order_db", snapshot.database)
        self.assertEqual("minos", snapshot.user)
        self.assertEqual("min0s", snapshot.password)
        self.assertEqual("localhost", snapshot.host)
        self.assertEqual(5432, snapshot.port)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment(self):
        config = MinosConfig(path=self.config_file_path)
        repository = config.repository
        self.assertEqual("foo", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment_false(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        repository = config.repository
        self.assertEqual("order_db", repository.database)

    def test_overwrite_with_parameter(self):
        config = MinosConfig(path=self.config_file_path, repository_database="foo")
        repository = config.repository
        self.assertEqual("foo", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_parameter_priority(self):
        config = MinosConfig(path=self.config_file_path, repository_database="bar")
        repository = config.repository
        self.assertEqual("bar", repository.database)

    def test_get_default_default(self):
        with MinosConfig(path=self.config_file_path) as config:
            self.assertEqual(config, MinosConfigAbstract.get_default())

    def test_multiple_default_config_raises(self):
        with self.assertRaises(MinosConfigDefaultAlreadySetException):
            with MinosConfig(path=self.config_file_path):
                with MinosConfig(path=self.config_file_path):
                    pass


if __name__ == "__main__":
    unittest.main()
