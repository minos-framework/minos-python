import unittest

from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    AiopgBrokerPublisherQueueDatabaseOperationFactory,
    BrokerPublisherQueue,
    DatabaseBrokerPublisherQueue,
    DatabaseBrokerQueue,
)
from tests.utils import (
    NetworksTestCase,
)


class TestPostgreSqlBrokerPublisherQueue(NetworksTestCase, DatabaseMinosTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(DatabaseBrokerPublisherQueue, (DatabaseBrokerQueue, BrokerPublisherQueue)))

    async def test_operation_factory(self):
        queue = DatabaseBrokerPublisherQueue.from_config(self.config)

        self.assertIsInstance(queue.operation_factory, AiopgBrokerPublisherQueueDatabaseOperationFactory)


class TestPostgreSqlBrokerPublisherQueueQueryFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerPublisherQueueDatabaseOperationFactory()

    def test_build_table_name(self):
        self.assertEqual("broker_publisher_queue", self.factory.build_table_name())


if __name__ == "__main__":
    unittest.main()
