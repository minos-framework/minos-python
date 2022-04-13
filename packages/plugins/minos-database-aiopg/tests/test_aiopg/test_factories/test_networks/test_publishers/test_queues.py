import unittest

from minos.networks import (
    BrokerPublisherQueueDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgBrokerPublisherQueueDatabaseOperationFactory,
    AiopgBrokerQueueDatabaseOperationFactory,
    AiopgDatabaseOperation,
)


class TestAiopgBrokerPublisherQueueDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerPublisherQueueDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(
            AiopgBrokerPublisherQueueDatabaseOperationFactory,
            (BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerQueueDatabaseOperationFactory),
        )

    def test_build_table_name(self):
        self.assertEqual("broker_publisher_queue", self.factory.build_table_name())

    def test_build_create_table(self):
        operation = self.factory.build_create_table()
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_update_not_processed(self):
        operation = self.factory.build_update_not_processed(id_=56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_delete_processed(self):
        operation = self.factory.build_delete_processed(id_=56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_mark_processing(self):
        operation = self.factory.build_mark_processing(ids={56, 78})
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_count_not_processed(self):
        operation = self.factory.build_count_not_processed(retry=3)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_insert(self):
        operation = self.factory.build_insert(topic="foo", data=bytes())
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_select_not_processed(self):
        operation = self.factory.build_select_not_processed(retry=3, records=1000)
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
