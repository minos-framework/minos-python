import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    Config,
    ConfigV2,
    MinosConfigException,
    PoolFactory,
)
from tests.utils import (
    BASE_PATH,
    FakeBrokerClientPool,
    FakeBrokerPort,
    FakeBrokerPublisher,
    FakeBrokerSubscriber,
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


class TestConfigV2(unittest.TestCase):
    def setUp(self) -> None:
        self.file_path = BASE_PATH / "config" / "v2.yml"
        self.config = ConfigV2(self.file_path)

    def test_is_subclass(self):
        self.assertTrue(issubclass(ConfigV2, Config))

    def test_aggregate(self):
        expected = {
            "entities": [int],
            "repositories": {
                "event": FakeEventRepository,
                "snapshot": FakeSnapshotRepository,
                "transaction": FakeTransactionRepository,
            },
        }
        self.assertEqual(expected, self.config.get_aggregate())

    def test_version(self):
        self.assertEqual(2, self.config.version)

    def test_name(self):
        self.assertEqual("Order", self.config.get_name())

    def test_injections(self):
        expected = [
            PoolFactory,
            FakeHttpConnector,
            FakeBrokerPublisher,
            FakeBrokerSubscriberBuilder(FakeBrokerSubscriber),
            FakeEventRepository,
            FakeSnapshotRepository,
            FakeTransactionRepository,
            FakeDiscoveryConnector,
            FakeSagaManager,
            FakeCustomInjection,
        ]
        self.assertEqual(expected, self.config.get_injections())

    def test_injections_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(list(), self.config.get_injections())

    def test_injections_not_injectable(self):
        with patch.object(ConfigV2, "_get_pools", return_value={"factory": int}):
            with self.assertRaises(MinosConfigException):
                self.config.get_injections()

    def test_interface_http(self):
        observed = self.config.get_interface_by_name("http")

        expected = {
            "port": FakeHttpPort,
            "connector": {
                "client": FakeHttpConnector,
                "host": "localhost",
                "port": 8900,
            },
        }
        self.assertEqual(expected, observed)

    def test_interface_http_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            with self.assertRaises(MinosConfigException):
                self.config.get_interface_by_name("http")

    def test_interface_broker(self):
        config = ConfigV2(self.file_path, with_environment=False)
        broker = config.get_interface_by_name("broker")

        expected = {
            "port": FakeBrokerPort,
            "common": {
                "host": "localhost",
                "port": 9092,
                "queue": {"records": 10, "retry": 2},
            },
            "publisher": {"client": FakeBrokerPublisher, "queue": int},
            "subscriber": {"client": FakeBrokerSubscriber, "queue": int, "validator": float},
        }

        self.assertEqual(expected, broker)

    def test_interface_broker_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            with self.assertRaises(MinosConfigException):
                self.config.get_interface_by_name("broker")

    def test_interface_periodic(self):
        observed = self.config.get_interface_by_name("periodic")

        expected = {
            "port": FakePeriodicPort,
        }
        self.assertEqual(expected, observed)

    def test_interface_periodic_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            with self.assertRaises(MinosConfigException):
                self.config.get_interface_by_name("periodic")

    def test_interface_unknown(self):
        config = ConfigV2(self.file_path, with_environment=False)
        with self.assertRaises(MinosConfigException):
            config.get_interface_by_name("unknown")

    def test_pools(self):
        expected = {
            "factory": PoolFactory,
            "types": {
                "broker": FakeBrokerClientPool,
                "database": FakeDatabasePool,
                "lock": FakeLockPool,
            },
        }
        self.assertEqual(expected, self.config.get_pools())

    def test_pools_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(dict(), self.config.get_pools())

    def test_services(self):
        self.assertEqual([float, int], self.config.get_services())

    def test_services_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(list(), self.config.get_services())

    def test_routers(self):
        self.assertEqual([set, dict], self.config.get_routers())

    def test_routers_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(list(), self.config.get_routers())

    def test_middleware(self):
        self.assertEqual([list, tuple], self.config.get_middleware())

    def test_middleware_not_defined(self):
        with patch.object(ConfigV2, "get_by_key", side_effect=MinosConfigException("")):
            self.assertEqual(list(), self.config.get_middleware())

    def test_saga(self):
        config = ConfigV2(self.file_path, with_environment=False)
        saga_config = config.get_saga()
        expected = {
            "manager": FakeSagaManager,
        }

        self.assertEqual(expected, saga_config)

    def test_database_default(self):
        config = ConfigV2(self.file_path, with_environment=False)
        database_config = config.get_default_database()
        self.assertEqual("order_db", database_config["database"])
        self.assertEqual("minos", database_config["user"])
        self.assertEqual("min0s", database_config["password"])
        self.assertEqual("localhost", database_config["host"])
        self.assertEqual(5432, database_config["port"])

    def test_database_query(self):
        config = ConfigV2(self.file_path, with_environment=False)
        query_database = config.get_database_by_name("query")
        self.assertEqual("order_query_db", query_database["database"])
        self.assertEqual("minos", query_database["user"])
        self.assertEqual("min0s", query_database["password"])
        self.assertEqual("localhost", query_database["host"])
        self.assertEqual(5432, query_database["port"])

    def test_database_saga(self):
        config = ConfigV2(self.file_path, with_environment=False)
        saga = config.get_database_by_name("saga")
        self.assertEqual("./order.lmdb", saga["path"])

    def test_discovery(self):
        config = ConfigV2(self.file_path, with_environment=False)
        observed = config.get_discovery()

        expected = {
            "connector": FakeDiscoveryConnector,
            "client": str,
            "host": "localhost",
            "port": 8080,
        }

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
