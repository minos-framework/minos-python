import unittest

from minos.common import (
    MinosConfig,
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
        self.assertEqual("src.aggregates.Order", service.aggregate)
        self.assertEqual(dict(), service.injections)
        self.assertEqual(list(), service.services)

    def test_config_rest(self):
        rest = self.config.rest

        self.assertEqual("localhost", rest.host)
        self.assertEqual(8900, rest.port)

    def test_config_events_service(self):
        events = self.config.events
        self.assertEqual("minos.services.CQRSService", events.service)

    def test_config_events_queue_database(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        broker = config.broker
        queue = broker.queue
        self.assertEqual("order_db", queue.database)
        self.assertEqual("minos", queue.user)
        self.assertEqual("min0s", queue.password)
        self.assertEqual("localhost", queue.host)
        self.assertEqual(5432, queue.port)
        self.assertEqual(10, queue.records)
        self.assertEqual(2, queue.retry)

    def test_config_commands_service(self):
        commands = self.config.commands
        self.assertEqual("minos.services.OrderService", commands.service)

    def test_config_queries_service(self):
        query = self.config.queries
        self.assertEqual("minos.services.OrderQueryService", query.service)

    def test_config_saga_storage(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        saga = config.saga
        storage = saga.storage
        self.assertEqual(BASE_PATH / "order.lmdb", storage.path)

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

    def test_config_discovery(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        discovery = config.discovery
        self.assertEqual("minos", discovery.client)
        self.assertEqual("localhost", discovery.host)
        self.assertEqual(8080, discovery.port)


if __name__ == "__main__":
    unittest.main()
