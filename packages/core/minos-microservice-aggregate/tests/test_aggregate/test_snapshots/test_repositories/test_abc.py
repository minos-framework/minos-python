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

from minos.aggregate import (
    Condition,
    Ordering,
    SnapshotEntry,
    SnapshotRepository,
)
from minos.aggregate.queries import (
    _EqualCondition,
)
from minos.common import (
    SetupMixin,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
)
from tests.utils import (
    AggregateTestCase,
    Car,
    FakeAsyncIterator,
)


class _SnapshotRepository(SnapshotRepository):
    """For testing purposes."""

    def _find_entries(self, *args, **kwargs) -> AsyncIterator[SnapshotEntry]:
        """For testing purposes."""

    async def _synchronize(self, **kwargs) -> None:
        """For testing purposes."""


class TestSnapshotRepository(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.snapshot_repository = _SnapshotRepository()
        self.entries = [SnapshotEntry.from_root_entity(Car(3, "red"))] * 5
        self.synchronize_mock = AsyncMock()
        self.find_mock = MagicMock(return_value=FakeAsyncIterator(self.entries))

        self.snapshot_repository._find_entries = self.find_mock
        self.snapshot_repository._synchronize = self.synchronize_mock

        self.classname = "path.to.Product"

    def test_subclass(self):
        self.assertTrue(issubclass(SnapshotRepository, (ABC, SetupMixin)))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_find_entries", "_synchronize"}, SnapshotRepository.__abstractmethods__)

    async def test_get(self):
        transaction = TransactionEntry()
        uuid = uuid4()
        observed = await self.snapshot_repository.get(self.classname, uuid, transaction)
        self.assertEqual(self.entries[0].build(), observed)

        self.assertEqual(1, self.synchronize_mock.call_count)
        self.assertEqual(call(synchronize=False), self.synchronize_mock.call_args)

        self.assertEqual(1, self.find_mock.call_count)
        args = call(
            name=self.classname,
            condition=_EqualCondition("uuid", uuid),
            ordering=None,
            limit=None,
            streaming_mode=False,
            transaction=transaction,
            exclude_deleted=False,
        )
        self.assertEqual(args, self.find_mock.call_args)

    async def test_get_transaction_null(self):
        await self.snapshot_repository.get(self.classname, uuid4())

        self.assertEqual(1, self.find_mock.call_count)
        self.assertEqual(None, self.find_mock.call_args.kwargs["transaction"])

    async def test_get_transaction_context(self):
        transaction = TransactionEntry()
        TRANSACTION_CONTEXT_VAR.set(transaction)
        await self.snapshot_repository.get(self.classname, uuid4())

        self.assertEqual(1, self.find_mock.call_count)
        self.assertEqual(transaction, self.find_mock.call_args.kwargs["transaction"])

    async def test_get_all(self):
        transaction = TransactionEntry()

        iterable = self.snapshot_repository.get_all(self.classname, Ordering.ASC("name"), 10, True, transaction)
        observed = [a async for a in iterable]
        self.assertEqual([e.build() for e in self.entries], observed)

        self.assertEqual(
            [
                call(
                    name=self.classname,
                    condition=Condition.TRUE,
                    ordering=Ordering.ASC("name"),
                    limit=10,
                    streaming_mode=True,
                    transaction=transaction,
                    exclude_deleted=True,
                )
            ],
            self.find_mock.call_args_list,
        )

    async def test_find(self):
        transaction = TransactionEntry()
        iterable = self.snapshot_repository.find(
            self.classname, Condition.TRUE, Ordering.ASC("name"), 10, True, transaction
        )
        observed = [a async for a in iterable]
        self.assertEqual([e.build() for e in self.entries], observed)

        self.assertEqual(1, self.synchronize_mock.call_count)
        self.assertEqual(call(synchronize=False), self.synchronize_mock.call_args)

        self.assertEqual(1, self.find_mock.call_count)
        args = call(
            name=self.classname,
            condition=Condition.TRUE,
            ordering=Ordering.ASC("name"),
            limit=10,
            streaming_mode=True,
            transaction=transaction,
            exclude_deleted=True,
        )
        self.assertEqual(args, self.find_mock.call_args)

    async def test_find_transaction_null(self):
        [a async for a in self.snapshot_repository.find(self.classname, Condition.TRUE)]

        self.assertEqual(1, self.find_mock.call_count)
        self.assertEqual(None, self.find_mock.call_args.kwargs["transaction"])

    async def test_find_transaction_context(self):
        transaction = TransactionEntry()
        TRANSACTION_CONTEXT_VAR.set(transaction)
        [a async for a in self.snapshot_repository.find(self.classname, Condition.TRUE)]

        self.assertEqual(1, self.find_mock.call_count)
        self.assertEqual(transaction, self.find_mock.call_args.kwargs["transaction"])

    async def test_synchronize(self):
        await self.snapshot_repository.synchronize()

        self.assertEqual(1, self.synchronize_mock.call_count)
        self.assertEqual(call(synchronize=False), self.synchronize_mock.call_args)


if __name__ == "__main__":
    unittest.main()
