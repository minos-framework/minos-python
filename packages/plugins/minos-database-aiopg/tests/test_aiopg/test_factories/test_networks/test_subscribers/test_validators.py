import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    ComposedDatabaseOperation,
)
from minos.networks import (
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
    AiopgDatabaseOperation,
)


class TestAiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(
            issubclass(
                AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
                BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
            )
        )

    def test_build_table_name(self):
        self.assertEqual("broker_subscriber_processed_messages", self.factory.build_table_name())

    def test_build_create_table(self):
        operation = self.factory.build_create_table()
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(2, len(operation.operations))
        for sub in operation.operations:
            self.assertIsInstance(sub, AiopgDatabaseOperation)

    def test_build_insert_row(self):
        operation = self.factory.build_insert_row("foo", uuid4())
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
