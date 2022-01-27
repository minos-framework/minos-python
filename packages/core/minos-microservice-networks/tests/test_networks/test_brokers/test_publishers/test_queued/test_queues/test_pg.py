import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueueQueryFactory,
    PostgreSqlBrokerQueue,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlBrokerPublisherQueue(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlBrokerPublisherQueue, (PostgreSqlBrokerQueue, BrokerPublisherQueue)))

    async def test_query_factory(self):
        queue = PostgreSqlBrokerPublisherQueue.from_config(self.config)

        self.assertIsInstance(queue.query_factory, PostgreSqlBrokerPublisherQueueQueryFactory)


class TestPostgreSqlBrokerPublisherQueueQueryFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = PostgreSqlBrokerPublisherQueueQueryFactory()

    def test_build_table_name(self):
        self.assertEqual("broker_publisher_queue", self.factory.build_table_name())


if __name__ == "__main__":
    unittest.main()
