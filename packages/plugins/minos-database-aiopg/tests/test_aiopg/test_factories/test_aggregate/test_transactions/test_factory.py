import unittest
from uuid import (
    uuid4,
)

from minos.aggregate import (
    TransactionDatabaseOperationFactory,
    TransactionStatus,
)
from minos.common import (
    ComposedDatabaseOperation,
    current_datetime,
)
from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
    AiopgTransactionDatabaseOperationFactory,
)


class TestAiopgTransactionDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgTransactionDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(issubclass(AiopgTransactionDatabaseOperationFactory, TransactionDatabaseOperationFactory))

    def test_build_table_name(self):
        self.assertEqual("aggregate_transaction", self.factory.build_table_name())

    def test_build_create(self):
        operation = self.factory.build_create()
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(3, len(operation.operations))
        for sub in operation.operations:
            self.assertIsInstance(sub, AiopgDatabaseOperation)

    def test_build_submit(self):
        operation = self.factory.build_submit(
            uuid=uuid4(),
            destination_uuid=uuid4(),
            status=TransactionStatus.COMMITTED,
            event_offset=234234,
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_query(self):
        operation = self.factory.build_query(
            uuid=uuid4(),
            uuid_ne=uuid4(),
            uuid_in={uuid4(), uuid4()},
            destination_uuid=uuid4(),
            status=TransactionStatus.COMMITTED,
            status_in={TransactionStatus.REJECTED, TransactionStatus.RESERVED},
            event_offset=234,
            event_offset_lt=24342,
            event_offset_gt=3424,
            event_offset_le=2342,
            event_offset_ge=234342,
            updated_at=current_datetime(),
            updated_at_lt=current_datetime(),
            updated_at_gt=current_datetime(),
            updated_at_le=current_datetime(),
            updated_at_ge=current_datetime(),
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
