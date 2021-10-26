import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    TRANSACTION_CONTEXT_VAR,
    Transaction,
    TransactionStatus,
)
from tests.utils import (
    FakeRepository,
    FakeTransactionRepository,
)


class TestTransaction(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.transaction_repository = FakeTransactionRepository()
        self.event_repository = FakeRepository(transaction_repository=self.transaction_repository)
        self.kwargs = {
            "event_repository": self.event_repository,
            "transaction_repository": self.transaction_repository,
        }

    def test_constructor(self):
        transaction = Transaction(**self.kwargs)

        self.assertIsInstance(transaction.uuid, UUID)
        self.assertEqual(TransactionStatus.CREATED, transaction.status)
        self.assertEqual(None, transaction.event_offset)
        self.assertEqual(True, transaction.autocommit)

        self.assertEqual(self.event_repository, transaction.event_repository)
        self.assertEqual(self.transaction_repository, transaction.transaction_repository)

    def test_constructor_extended(self):
        uuid = uuid4()
        status = TransactionStatus.PENDING
        event_offset = 56
        transaction = Transaction(uuid, status, event_offset, autocommit=False, **self.kwargs)
        self.assertEqual(uuid, transaction.uuid)
        self.assertEqual(status, transaction.status)
        self.assertEqual(event_offset, transaction.event_offset)
        self.assertEqual(False, transaction.autocommit)

        self.assertEqual(self.event_repository, transaction.event_repository)
        self.assertEqual(self.transaction_repository, transaction.transaction_repository)

    def test_constructor_raw_status(self):
        transaction = Transaction(status="pending", **self.kwargs)
        self.assertEqual(TransactionStatus.PENDING, transaction.status)

    async def test_async_context_manager_with_context_var(self):
        self.assertEqual(TRANSACTION_CONTEXT_VAR.get(), None)

        async with Transaction(**self.kwargs) as transaction:
            self.assertEqual(TRANSACTION_CONTEXT_VAR.get(), transaction)

        self.assertEqual(TRANSACTION_CONTEXT_VAR.get(), None)

    async def test_async_context_manager(self):
        with patch("minos.common.Transaction.save") as save_mock, patch(
            "minos.common.Transaction.commit"
        ) as commit_mock:
            async with Transaction(**self.kwargs):
                self.assertEqual(1, save_mock.call_count)
                self.assertEqual(0, commit_mock.call_count)

            self.assertEqual(0, commit_mock.call_count)

    async def test_async_context_manager_with_changes(self):
        with patch("minos.common.Transaction.commit") as commit_mock:
            async with Transaction(**self.kwargs) as transaction:
                self.assertEqual(0, commit_mock.call_count)
                transaction.status = TransactionStatus.PENDING

            self.assertEqual(1, commit_mock.call_count)

            commit_mock.reset_mock()

        with patch("minos.common.Transaction.commit") as commit_mock:
            async with Transaction(**self.kwargs) as transaction:
                self.assertEqual(0, commit_mock.call_count)
                transaction.status = TransactionStatus.RESERVED

            self.assertEqual(1, commit_mock.call_count)

            commit_mock.reset_mock()

    async def test_async_context_manager_without_autocommit(self):
        with patch("minos.common.Transaction.commit") as commit_mock:
            async with Transaction(autocommit=False, **self.kwargs) as transaction:
                self.assertEqual(0, commit_mock.call_count)
                transaction.status = TransactionStatus.PENDING

            self.assertEqual(0, commit_mock.call_count)

    async def test_async_context_manager_raises(self):
        with self.assertRaises(ValueError):
            async with Transaction(status=TransactionStatus.COMMITTED, **self.kwargs):
                pass

        with self.assertRaises(ValueError):
            async with Transaction(status=TransactionStatus.RESERVED, **self.kwargs):
                pass

        with self.assertRaises(ValueError):
            async with Transaction(status=TransactionStatus.REJECTED, **self.kwargs):
                pass

        TRANSACTION_CONTEXT_VAR.set(Transaction(**self.kwargs))
        with self.assertRaises(ValueError):
            async with Transaction(**self.kwargs):
                pass


class TestTransactionStatus(unittest.TestCase):
    def test_value_of_created(self):
        self.assertEqual(TransactionStatus.CREATED, TransactionStatus.value_of("created"))

    def test_value_of_pending(self):
        self.assertEqual(TransactionStatus.PENDING, TransactionStatus.value_of("pending"))

    def test_value_of_reserved(self):
        self.assertEqual(TransactionStatus.RESERVED, TransactionStatus.value_of("reserved"))

    def test_value_of_committed(self):
        self.assertEqual(TransactionStatus.COMMITTED, TransactionStatus.value_of("committed"))

    def test_value_of_rejected(self):
        self.assertEqual(TransactionStatus.REJECTED, TransactionStatus.value_of("rejected"))

    def test_value_of_raises(self):
        with self.assertRaises(ValueError):
            TransactionStatus.value_of("foo")


if __name__ == "__main__":
    unittest.main()
