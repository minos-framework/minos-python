import unittest
from abc import (
    ABC,
)
from collections.abc import (
    AsyncIterator,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common import (
    NotProvidedException,
    SetupMixin,
)
from minos.transactions import (
    TransactionalMixin,
    TransactionEntry,
    TransactionNotFoundException,
    TransactionRepository,
    TransactionStatus,
)
from tests.utils import (
    FakeAsyncIterator,
    FakeLock,
    TransactionsTestCase,
)


class _TransactionRepository(TransactionRepository):
    """For testing purposes."""

    async def _submit(self, transaction: TransactionEntry) -> None:
        """For testing purposes."""

    def _select(self, **kwargs) -> AsyncIterator[TransactionEntry]:
        """For testing purposes."""


class TestTransactionRepository(TransactionsTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.transaction_repository = _TransactionRepository()

    async def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyArgumentEqualDefault
            _TransactionRepository(lock_pool=None, pool_factory=None)

    def test_abstract(self):
        self.assertTrue(issubclass(TransactionRepository, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select"}, TransactionRepository.__abstractmethods__)

    def test_observers_empty(self):
        self.assertEqual(set(), self.transaction_repository.observers)

    def test_register_observer(self):
        observer1, observer2 = TransactionalMixin(), TransactionalMixin()
        self.transaction_repository.register_observer(observer1)
        self.transaction_repository.register_observer(observer2)
        self.assertEqual({observer1, observer2}, self.transaction_repository.observers)

    def test_unregister_observer(self):
        observer1, observer2 = TransactionalMixin(), TransactionalMixin()
        self.transaction_repository.register_observer(observer1)
        self.transaction_repository.register_observer(observer2)
        self.assertEqual({observer1, observer2}, self.transaction_repository.observers)
        self.transaction_repository.unregister_observer(observer2)
        self.assertEqual({observer1}, self.transaction_repository.observers)

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

        self.pool_factory.get_pool("lock").acquire = mock

        self.assertEqual(expected, self.transaction_repository.write_lock())
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("aggregate_transaction_write_lock"), mock.call_args)

    async def test_get(self):
        mock = MagicMock(return_value=FakeAsyncIterator([1]))
        self.transaction_repository.select = mock
        uuid = uuid4()

        observed = await self.transaction_repository.get(uuid)

        self.assertEqual(1, observed)
        self.assertEqual([call(uuid=uuid)], mock.call_args_list)

    async def test_get_raises(self):
        mock = MagicMock(return_value=FakeAsyncIterator([]))
        self.transaction_repository.select = mock
        with self.assertRaises(TransactionNotFoundException):
            await self.transaction_repository.get(uuid4())

    async def test_select(self):
        uuid = uuid4()

        mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.transaction_repository._select = mock

        iterable = self.transaction_repository.select(
            uuid=uuid, status_in=(TransactionStatus.REJECTED, TransactionStatus.COMMITTED)
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
            updated_at=None,
            updated_at_lt=None,
            updated_at_gt=None,
            updated_at_le=None,
            updated_at_ge=None,
        )
        self.assertEqual(args, mock.call_args)


if __name__ == "__main__":
    unittest.main()
