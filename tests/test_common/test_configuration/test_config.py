"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

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
        self.config_file_path = BASE_PATH / "test_config.yaml"

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

    def test_config_events_database(self):
        config = MinosConfig(path=self.config_file_path)
        events = config.events
        database = events.database
        self.assertEqual("./tests/local_db.lmdb", database.path)
        self.assertEqual("database_events_test", database.name)

    def test_config_events_queue_database(self):
        config = MinosConfig(path=self.config_file_path)
        events = config.events
        queue = events.queue
        self.assertEqual("broker_db", queue.database)
        self.assertEqual("broker", queue.user)
        self.assertEqual("br0k3r", queue.password)
        self.assertEqual("localhost", queue.host)
        self.assertEqual(5432, queue.port)
        self.assertEqual(10, queue.records)
        self.assertEqual(2, queue.retry)

    def test_config_commands_database(self):
        config = MinosConfig(path=self.config_file_path)
        commands = config.commands
        database = commands.database
        self.assertEqual("./tests/local_db.lmdb", database.path)
        self.assertEqual("database_commands_test", database.name)

    def test_config_commands_queue_database(self):
        config = MinosConfig(path=self.config_file_path)
        commands = config.commands
        queue = commands.queue
        self.assertEqual("broker_db", queue.database)
        self.assertEqual("broker", queue.user)
        self.assertEqual("br0k3r", queue.password)
        self.assertEqual("localhost", queue.host)
        self.assertEqual(5432, queue.port)
        self.assertEqual(10, queue.records)
        self.assertEqual(2, queue.retry)

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
