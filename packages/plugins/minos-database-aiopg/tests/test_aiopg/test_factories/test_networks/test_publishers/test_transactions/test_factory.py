import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    ComposedDatabaseOperation,
)
from minos.networks import (
    BrokerPublisherTransactionDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgBrokerPublisherTransactionDatabaseOperationFactory,
    AiopgDatabaseOperation,
)


# noinspection SqlNoDataSourceInspection
class TestBrokerPublisherTransactionRepositoryTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerPublisherTransactionDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(
            issubclass(
                AiopgBrokerPublisherTransactionDatabaseOperationFactory,
                BrokerPublisherTransactionDatabaseOperationFactory,
            )
        )

    def test_table_name(self):
        observed = self.factory.build_table_name()
        self.assertEqual("broker_publisher_transactional_messages", observed)

    def test_create(self):
        observed = self.factory.build_create()
        self.assertIsInstance(observed, ComposedDatabaseOperation)
        self.assertEqual(2, len(observed.operations))
        for operation in observed.operations:
            self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_query(self):
        observed = self.factory.build_query(uuid4())
        self.assertIsInstance(observed, AiopgDatabaseOperation)

    def test_build_query_all(self):
        observed = self.factory.build_query(None)
        self.assertIsInstance(observed, AiopgDatabaseOperation)

    def test_submit(self):
        observed = self.factory.build_submit(bytes(), uuid4())
        self.assertIsInstance(observed, AiopgDatabaseOperation)

    def test_delete_batch(self):
        observed = self.factory.build_delete_batch(uuid4())
        self.assertIsInstance(observed, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
