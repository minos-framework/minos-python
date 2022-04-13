import unittest

from minos.networks import (
    BrokerQueueDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgBrokerQueueDatabaseOperationFactory,
    AiopgDatabaseOperation,
)


class _BrokerQueueDatabaseOperationFactory(AiopgBrokerQueueDatabaseOperationFactory):
    """For testing purposes."""

    def build_table_name(self) -> str:
        """For testing purposes."""
        return "foo"


class TestAiopgBrokerQueueDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = _BrokerQueueDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(AiopgBrokerQueueDatabaseOperationFactory, BrokerQueueDatabaseOperationFactory)

    def test_build_table_name(self):
        self.assertEqual("foo", self.factory.build_table_name())

    def test_build_create(self):
        operation = self.factory.build_create()
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_mark_processed(self):
        operation = self.factory.build_mark_processed(id_=56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_delete(self):
        operation = self.factory.build_delete(id_=56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_mark_processing(self):
        operation = self.factory.build_mark_processing(ids={56, 78})
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_count(self):
        operation = self.factory.build_count(retry=3)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_submit(self):
        operation = self.factory.build_submit(topic="foo", data=bytes())
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_query(self):
        operation = self.factory.build_query(retry=3, records=1000)
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
