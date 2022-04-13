import unittest

from minos.networks import (
    BrokerSubscriberQueueDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgBrokerQueueDatabaseOperationFactory,
    AiopgBrokerSubscriberQueueDatabaseOperationFactory,
    AiopgDatabaseOperation,
)


class TestAiopgBrokerSubscriberQueueDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerSubscriberQueueDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(
            issubclass(
                AiopgBrokerSubscriberQueueDatabaseOperationFactory,
                (BrokerSubscriberQueueDatabaseOperationFactory, AiopgBrokerQueueDatabaseOperationFactory),
            )
        )

    def test_build_table_name(self):
        self.assertEqual("broker_subscriber_queue", self.factory.build_table_name())

    def test_build_count_not_processed(self):
        operation = self.factory.build_count_not_processed(retry=3, topics={"foo", "bar"})
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_select_not_processed(self):
        operation = self.factory.build_select_not_processed(retry=3, records=100, topics={"foo", "bar"})
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
