import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    MinosLockPoolNotProvidedException,
    TransactionEntry,
    TransactionRepository,
    TransactionStatus,
)
from minos.common import (
    MinosSetup,
)
from tests.utils import (
    FakeAsyncIterator,
    FakeLock,
    FakeTransactionRepository,
    MinosTestCase,
)


class TestTransactionRepository(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.transaction_repository = FakeTransactionRepository()

    async def test_constructor_raises(self):
        with self.assertRaises(MinosLockPoolNotProvidedException):
            # noinspection PyTypeChecker
            FakeTransactionRepository(lock_pool=None)

    def test_abstract(self):
        self.assertTrue(issubclass(TransactionRepository, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select"}, TransactionRepository.__abstractmethods__)

    async def test_submit(self):
        transaction = TransactionEntry()
        mock = AsyncMock()
        self.transaction_repository._submit = mock

        await self.transaction_repository.submit(transaction)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(transaction), mock.call_args)

    def test_write_lock(self):
        expected = FakeLock()
        mock = MagicMock(return_value=expected)

        self.lock_pool.acquire = mock

        self.assertEqual(expected, self.transaction_repository.write_lock())
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("aggregate_transaction_write_lock"), mock.call_args)

    async def test_select(self):
        uuid = uuid4()

        mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.transaction_repository._select = mock

        iterable = self.transaction_repository.select(
            uuid=uuid, status_in=(TransactionStatus.REJECTED, TransactionStatus.COMMITTED), event_offset_gt=56
        )
        observed = [v async for v in iterable]

        self.assertEqual(list(range(5)), observed)
        self.assertEqual(1, mock.call_count)
        args = call(
            uuid=uuid,
            uuid_ne=None,
            uuid_in=None,
            destination_uuid=None,
            status=None,
            status_in=(TransactionStatus.REJECTED, TransactionStatus.COMMITTED),
            event_offset=None,
            event_offset_lt=None,
            event_offset_gt=56,
            event_offset_le=None,
            event_offset_ge=None,
        )
        self.assertEqual(args, mock.call_args)


if __name__ == "__main__":
    unittest.main()
