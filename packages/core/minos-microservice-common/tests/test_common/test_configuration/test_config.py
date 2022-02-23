import unittest
from unittest.mock import (
    patch,
)

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

    def test_services(self):
        self.assertEqual(["tests.services.OrderService", "tests.services.OrderQueryService"], self.config.services)

    def test_services_not_defined(self):
        with patch("minos.common.MinosConfig._get", side_effect=MinosConfigException("")):
            self.assertEqual([], self.config.services)

    def test_middleware(self):
        self.assertEqual(["tests.middleware.performance_tracking"], self.config.middleware)

    def test_middleware_not_defined(self):
        with patch("minos.common.MinosConfig._get", side_effect=MinosConfigException("")):
            self.assertEqual([], self.config.middleware)

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

    def test_config_query_repository(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        query_repository = config.query_repository
        self.assertEqual("order_query_db", query_repository.database)
        self.assertEqual("minos", query_repository.user)
        self.assertEqual("min0s", query_repository.password)
        self.assertEqual("localhost", query_repository.host)
        self.assertEqual(5432, query_repository.port)

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

    def test_config_decorators(self):
        config = MinosConfig(path=self.config_file_path, with_environment=False)
        decorators = config.decorators
        self.assertIsInstance(decorators, list)
        decorator = decorators[0]
        self.assertEqual(decorator.name, "rest")
        self.assertEqual(decorator.location, "minos.plugins.rest_aiohttp.RestEnroute")


if __name__ == "__main__":
    unittest.main()
