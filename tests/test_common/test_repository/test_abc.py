import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    PropertyMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common import (
    NULL_UUID,
    Action,
    MinosBrokerNotProvidedException,
    MinosLockPoolNotProvidedException,
    MinosRepository,
    MinosSetup,
    MinosTransactionRepositoryNotProvidedException,
    RepositoryEntry,
    Transaction,
    TransactionStatus,
)
from tests.utils import (
    FakeAsyncIterator,
    FakeLock,
    FakeRepository,
    MinosTestCase,
)


class TestMinosRepository(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.repository = FakeRepository()

    def test_subclass(self):
        self.assertTrue(issubclass(MinosRepository, (ABC, MinosSetup)))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select", "_offset"}, MinosRepository.__abstractmethods__)

    def test_constructor(self):
        repository = FakeRepository(
            event_broker=self.event_broker, transaction_repository=self.transaction_repository, lock_pool=self.lock_pool
        )
        self.assertEqual(self.event_broker, repository._event_broker)
        self.assertEqual(self.transaction_repository, repository._transaction_repository)
        self.assertEqual(self.lock_pool, repository._lock_pool)

    async def test_constructor_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            # noinspection PyTypeChecker
            FakeRepository(event_broker=None)
        with self.assertRaises(MinosTransactionRepositoryNotProvidedException):
            # noinspection PyTypeChecker
            FakeRepository(transaction_repository=None)
        with self.assertRaises(MinosLockPoolNotProvidedException):
            # noinspection PyTypeChecker
            FakeRepository(lock_pool=None)

    def test_transaction(self):
        uuid = uuid4()
        transaction = self.repository.transaction(uuid=uuid)
        self.assertEqual(Transaction(uuid), transaction)
        self.assertEqual(self.repository, transaction.event_repository)
        self.assertEqual(self.transaction_repository, transaction.transaction_repository)

    def test_check_transaction(self):
        pass

    def test_commit_transaction(self):
        pass

    async def test_create(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.repository.submit = mock

        entry = RepositoryEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.repository.create(entry))

        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_update(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.repository.submit = mock

        entry = RepositoryEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.repository.update(entry))

        self.assertEqual(Action.UPDATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_delete(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.repository.submit = mock

        entry = RepositoryEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.repository.delete(entry))

        self.assertEqual(Action.DELETE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    def test_submit(self):
        pass

    async def test_validate_true(self):
        aggregate_uuid = uuid4()
        transaction_uuid = uuid4()

        events = [
            RepositoryEntry(aggregate_uuid, "example.Car", 1),
            RepositoryEntry(aggregate_uuid, "example.Car", 2, transaction_uuid=transaction_uuid),
        ]
        transactions = []

        select_event_mock = PropertyMock(return_value=FakeAsyncIterator(events))
        self.repository.select = select_event_mock

        select_transaction_mock = PropertyMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = RepositoryEntry(aggregate_uuid, "example.Car")

        self.assertTrue(await self.repository.validate(entry))

        self.assertEqual(1, select_event_mock.call_count)
        self.assertEqual(call(aggregate_uuid=aggregate_uuid, transaction_uuid_ne=None), select_event_mock.call_args)

        self.assertEqual(1, select_transaction_mock.call_count)
        self.assertEqual(
            call(uuid_in=(NULL_UUID, transaction_uuid), status=TransactionStatus.RESERVED),
            select_transaction_mock.call_args,
        )

    async def test_validate_with_skip(self):
        aggregate_uuid = uuid4()
        transaction_uuid = uuid4()

        events = [
            RepositoryEntry(aggregate_uuid, "example.Car", 1),
            RepositoryEntry(aggregate_uuid, "example.Car", 2, transaction_uuid=transaction_uuid),
        ]
        transactions = []

        select_event_mock = PropertyMock(return_value=FakeAsyncIterator(events))
        self.repository.select = select_event_mock

        select_transaction_mock = PropertyMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = RepositoryEntry(aggregate_uuid, "example.Car")
        self.assertTrue(await self.repository.validate(entry, transaction_uuid_ne=transaction_uuid))

        self.assertEqual(1, select_event_mock.call_count)
        self.assertEqual(
            call(aggregate_uuid=aggregate_uuid, transaction_uuid_ne=transaction_uuid), select_event_mock.call_args
        )

    async def test_validate_false(self):
        aggregate_uuid = uuid4()
        transaction_uuid = uuid4()

        events = [
            RepositoryEntry(aggregate_uuid, "example.Car", 1),
            RepositoryEntry(aggregate_uuid, "example.Car", 2, transaction_uuid=transaction_uuid),
        ]
        transactions = [Transaction(transaction_uuid, TransactionStatus.RESERVED)]

        select_event_mock = PropertyMock(return_value=FakeAsyncIterator(events))
        self.repository.select = select_event_mock

        select_transaction_mock = PropertyMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = RepositoryEntry(aggregate_uuid, "example.Car")

        self.assertFalse(await self.repository.validate(entry))

        self.assertEqual(1, select_event_mock.call_count)
        self.assertEqual(call(aggregate_uuid=aggregate_uuid, transaction_uuid_ne=None), select_event_mock.call_args)

        self.assertEqual(1, select_transaction_mock.call_count)
        self.assertEqual(
            call(uuid_in=(NULL_UUID, transaction_uuid), status=TransactionStatus.RESERVED),
            select_transaction_mock.call_args,
        )

    def test_write_lock(self):
        expected = FakeLock()
        mock = MagicMock(return_value=expected)

        self.lock_pool.acquire = mock

        self.assertEqual(expected, self.repository.write_lock())
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("aggregate_event_write_lock"), mock.call_args)

    async def test_select(self):
        mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.repository._select = mock

        aggregate_uuid = uuid4()
        aggregate_name = "path.to.Aggregate"

        transaction_uuid = uuid4()
        iterable = self.repository.select(
            aggregate_uuid=aggregate_uuid, aggregate_name=aggregate_name, id_gt=56, transaction_uuid=transaction_uuid,
        )
        observed = [a async for a in iterable]
        self.assertEqual(list(range(5)), observed)

        self.assertEqual(1, mock.call_count)
        args = call(
            aggregate_uuid=aggregate_uuid,
            aggregate_name="path.to.Aggregate",
            version=None,
            version_lt=None,
            version_gt=None,
            version_le=None,
            version_ge=None,
            id=None,
            id_lt=None,
            id_gt=56,
            id_le=None,
            id_ge=None,
            transaction_uuid=transaction_uuid,
            transaction_uuid_ne=None,
        )

        self.assertEqual(args, mock.call_args)

    async def test_offset(self):
        mock = PropertyMock(side_effect=AsyncMock(return_value=56))
        # noinspection PyPropertyAccess
        type(self.repository)._offset = mock

        self.assertEqual(56, await self.repository.offset)
        self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
