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
        self.config_file_path = BASE_PATH / "test_config.yml"
        self.config = MinosConfig(path=self.config_file_path)

    def test_config_ini_fail(self):
        with self.assertRaises(MinosConfigException):
            MinosConfig(path=BASE_PATH / "test_fail_config.yaml")

    def test_cast_path(self):
        config_path = self.config._path
        self.assertEqual(self.config_file_path, config_path)

    def test_config_service(self):
        service = self.config.service
        self.assertEqual("Order", service.name)

    def test_config_rest(self):
        rest = self.config.rest

        broker = rest.broker
        self.assertEqual("localhost", broker.host)
        self.assertEqual(8900, broker.port)

        endpoints = rest.endpoints
        self.assertEqual("AddOrder", endpoints[0].name)

    def test_config_saga_broker(self):
        saga = self.config.saga

        broker = saga.broker
        self.assertEqual("localhost", broker.host)
        self.assertEqual(8900, broker.port)

    def test_config_events(self):
        events = self.config.events
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

    def test_config_saga_storage(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        saga = config.saga
        storage = saga.storage
        self.assertEqual(BASE_PATH / "order.lmdb", storage.path)

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

    def test_get_default_default(self):
        with MinosConfig(path=self.config_file_path) as config:
            self.assertEqual(config, MinosConfigAbstract.get_default())

    def test_multiple_default_config_raises(self):
        with self.assertRaises(MinosConfigDefaultAlreadySetException):
            with MinosConfig(path=self.config_file_path):
                with MinosConfig(path=self.config_file_path):
                    pass
