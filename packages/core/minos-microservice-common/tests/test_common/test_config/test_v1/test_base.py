import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    Config,
    ConfigV1,
    MinosConfigException,
)
from tests.utils import (
    BASE_PATH,
    FakeBrokerClientPool,
    FakeBrokerPort,
    FakeBrokerPublisher,
    FakeBrokerSubscriberBuilder,
    FakeCustomInjection,
    FakeDatabasePool,
    FakeDiscoveryConnector,
    FakeEventRepository,
    FakeHttpConnector,
    FakeHttpPort,
    FakeLockPool,
    FakePeriodicPort,
    FakeSagaManager,
    FakeSnapshotRepository,
    FakeTransactionRepository,
)


class TestConfigV1(unittest.TestCase):
    def setUp(self) -> None:
        self.file_path = BASE_PATH / "config" / "v1.yml"
        self.config = ConfigV1(self.file_path)

    def test_is_subclass(self):
        self.assertTrue(issubclass(ConfigV1, Config))

    def test_aggregate(self):
        expected = {
            "entities": [int],
            "repositories": dict(),
        }
        self.assertEqual(expected, self.config.get_aggregate())

    def test_version(self):
        self.assertEqual(1, self.config.version)

    def test_name(self):
        self.assertEqual("Order", self.config.get_name())

    def test_injections(self):
        expected = [
            FakeLockPool,
            FakeDatabasePool,
            FakeBrokerClientPool,
            FakeHttpConnector,
            FakeBrokerPublisher,
            FakeBrokerSubscriberBuilder,
            FakeEventRepository,
            FakeSnapshotRepository,
            FakeTransactionRepository,
            FakeDiscoveryConnector,
            FakeSagaManager,
            FakeCustomInjection,
        ]
        self.assertEqual(expected, self.config.get_injections())

    def test_interface_http(self):
        observed = self.config.get_interface_by_name("http")

        expected = {
            "port": FakeHttpPort,
            "connector": {
                "host": "localhost",
                "port": 8900,
            },
        }
        self.assertEqual(expected, observed)

    def test_interface_http_not_defined(self):
        with patch.object(ConfigV1, "get_by_key", side_effect=MinosConfigException("")):
            with self.assertRaises(MinosConfigException):
                self.config.get_interface_by_name("http")

    def test_interface_broker(self):
        config = ConfigV1(self.file_path, with_environment=False)
        broker = config.get_interface_by_name("broker")

        expected = {
            "port": FakeBrokerPort,
            "common": {
                "host": "localhost",
                "port": 9092,
                "queue": {"records": 10, "retry": 2},
            },
            "publisher": {},
            "subscriber": {},
        }

        self.assertEqual(expected, broker)

    def test_interface_broker_not_defined(self):
        with patch.object(ConfigV1, "get_by_key", side_effect=MinosConfigException("")):
            with self.assertRaises(MinosConfigException):
                self.config.get_interface_by_name("broker")

    def test_interface_periodic(self):
        observed = self.config.get_interface_by_name("periodic")

        expected = {
            "port": FakePeriodicPort,
        }
        self.assertEqual(expected, observed)

    def test_interface_periodic_not_defined(self):
        with patch.object(ConfigV1, "get_by_key", side_effect=MinosConfigException("")):
            with self.assertRaises(MinosConfigException):
                self.config.get_interface_by_name("periodic")

    def test_interface_unknown(self):
        config = ConfigV1(self.file_path, with_environment=False)
        with self.assertRaises(MinosConfigException):
            config.get_interface_by_name("unknown")

    def test_services(self):
        self.assertEqual([float, int], self.config.get_services())

    def test_services_not_defined(self):
        with patch.object(ConfigV1, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(list(), self.config.get_services())

    def test_routers(self):
        self.assertEqual([set, dict], self.config.get_routers())

    def test_routers_not_defined(self):
        with patch.object(ConfigV1, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(list(), self.config.get_routers())

    def test_middleware(self):
        self.assertEqual([list, tuple], self.config.get_middleware())

    def test_middleware_not_defined(self):
        with patch.object(ConfigV1, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(list(), self.config.get_middleware())

    def test_saga(self):
        config = ConfigV1(self.file_path, with_environment=False)
        saga_config = config.get_saga()
        expected = dict()

        self.assertEqual(expected, saga_config)

    def test_saga_not_defined(self):
        with patch.object(ConfigV1, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(dict(), self.config.get_saga())

    def test_database_default(self):
        config = ConfigV1(self.file_path, with_environment=False)
        database_config = config.get_default_database()
        self.assertEqual("order_db", database_config["database"])
        self.assertEqual("minos", database_config["user"])
        self.assertEqual("min0s", database_config["password"])
        self.assertEqual("localhost", database_config["host"])
        self.assertEqual(5432, database_config["port"])

    def test_database_event(self):
        config = ConfigV1(self.file_path, with_environment=False)
        database_config = config.get_database_by_name("event")
        self.assertEqual("order_db", database_config["database"])
        self.assertEqual("minos", database_config["user"])
        self.assertEqual("min0s", database_config["password"])
        self.assertEqual("localhost", database_config["host"])
        self.assertEqual(5432, database_config["port"])

    def test_database_query(self):
        config = ConfigV1(self.file_path, with_environment=False)
        query_database = config.get_database_by_name("query")
        self.assertEqual("order_query_db", query_database["database"])
        self.assertEqual("minos", query_database["user"])
        self.assertEqual("min0s", query_database["password"])
        self.assertEqual("localhost", query_database["host"])
        self.assertEqual(5432, query_database["port"])

    def test_database_snapshot(self):
        config = ConfigV1(self.file_path, with_environment=False)
        snapshot = config.get_database_by_name("snapshot")
        self.assertEqual("order_db", snapshot["database"])
        self.assertEqual("minos", snapshot["user"])
        self.assertEqual("min0s", snapshot["password"])
        self.assertEqual("localhost", snapshot["host"])
        self.assertEqual(5432, snapshot["port"])

    def test_database_broker(self):
        config = ConfigV1(self.file_path, with_environment=False)
        snapshot = config.get_database_by_name("broker")
        self.assertEqual("order_db", snapshot["database"])
        self.assertEqual("minos", snapshot["user"])
        self.assertEqual("min0s", snapshot["password"])
        self.assertEqual("localhost", snapshot["host"])
        self.assertEqual(5432, snapshot["port"])

    def test_database_saga(self):
        config = ConfigV1(self.file_path, with_environment=False)
        saga = config.get_database_by_name("saga")
        self.assertEqual(self.file_path.parent / "order.lmdb", saga["path"])

    def test_discovery(self):
        config = ConfigV1(self.file_path, with_environment=False)
        observed = config.get_discovery()

        expected = {
            "client": str,
            "host": "localhost",
            "port": 8080,
        }

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
