import unittest

from minos.plugins.aiopg import (
    AiopgBrokerSubscriberQueueDatabaseOperationFactory,
)


class TestAiopgBrokerSubscriberQueueDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerSubscriberQueueDatabaseOperationFactory()

    def test_build_table_name(self):
        self.assertEqual("broker_subscriber_queue", self.factory.build_table_name())


if __name__ == "__main__":
    unittest.main()
