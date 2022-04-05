import unittest
from abc import (
    ABC,
)

from minos.common import (
    AiopgDatabaseClient,
    BuildableMixin,
    DatabaseClient,
    DatabaseClientBuilder,
)
from tests.utils import (
    CommonTestCase,
)


class TestDatabaseClient(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(DatabaseClient, (ABC, BuildableMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"notifications", "is_valid", "execute", "fetch_all"}, DatabaseClient.__abstractmethods__)

    def test_get_builder(self):
        self.assertIsInstance(DatabaseClient.get_builder(), DatabaseClientBuilder)


class TestDatabaseClientBuilder(CommonTestCase):
    def test_with_name(self):
        builder = DatabaseClientBuilder(AiopgDatabaseClient).with_name("query")
        self.assertEqual({"name": "query"}, builder.kwargs)

    def test_with_config(self):
        builder = DatabaseClientBuilder(AiopgDatabaseClient).with_name("query").with_config(self.config)
        self.assertEqual({"name": "query"} | self.config.get_database_by_name("query"), builder.kwargs)

    def test_build(self):
        builder = DatabaseClientBuilder(AiopgDatabaseClient).with_name("query").with_config(self.config)
        client = builder.build()

        self.assertIsInstance(client, AiopgDatabaseClient)
        self.assertEqual(self.config.get_database_by_name("query")["database"], client.database)


if __name__ == "__main__":
    unittest.main()
