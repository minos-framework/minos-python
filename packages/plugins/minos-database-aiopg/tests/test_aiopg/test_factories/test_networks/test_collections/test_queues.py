import unittest

from minos.networks import (
    BrokerQueueDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgBrokerQueueDatabaseOperationFactory,
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


if __name__ == "__main__":
    unittest.main()
