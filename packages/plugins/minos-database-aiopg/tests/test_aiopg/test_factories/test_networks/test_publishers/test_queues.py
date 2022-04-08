import unittest

from minos.networks import (
    BrokerPublisherQueueDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgBrokerPublisherQueueDatabaseOperationFactory,
    AiopgBrokerQueueDatabaseOperationFactory,
)


class TestAiopgBrokerPublisherQueueDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerPublisherQueueDatabaseOperationFactory()

    def test_subclass(self):
        self.assertTrue(
            AiopgBrokerPublisherQueueDatabaseOperationFactory,
            (BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerQueueDatabaseOperationFactory),
        )

    def test_build_table_name(self):
        self.assertEqual("broker_publisher_queue", self.factory.build_table_name())


if __name__ == "__main__":
    unittest.main()
