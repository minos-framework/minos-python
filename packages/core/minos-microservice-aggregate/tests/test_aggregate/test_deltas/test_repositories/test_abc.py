from datetime import datetime
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
    PropertyMock,
    call,
    patch,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
    Action,
    Delta,
    DeltaEntry,
    DeltaRepository,
    DeltaRepositoryConflictException,
    DeltaRepositoryException,
    FieldDiff,
    FieldDiffContainer,
)
from minos.common import (
    NULL_UUID,
    NotProvidedException,
    SetupMixin,
    classname,
    current_datetime,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
    TransactionStatus,
)
from tests.utils import (
    AggregateTestCase,
    Car,
    FakeAsyncIterator,
    FakeLock,
)


class _DeltaRepository(DeltaRepository):
    """For testing purposes."""

    async def _submit(self, entry: DeltaEntry, **kwargs) -> DeltaEntry:
        """For testing purposes."""

    def _select(self, *args, **kwargs) -> AsyncIterator[DeltaEntry]:
        """For testing purposes."""

    @property
    async def _offset(self) -> int:
        """For testing purposes."""
        return 0


class TestDeltaRepository(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.delta_repository = _DeltaRepository()

    def test_subclass(self):
        self.assertTrue(issubclass(DeltaRepository, (ABC, SetupMixin)))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select", "_offset"}, DeltaRepository.__abstractmethods__)

    def test_constructor(self):
        repository = _DeltaRepository()
        self.assertEqual(self.transaction_repository, repository.transaction_repository)
        self.assertEqual(self.pool_factory.get_pool("lock"), repository._lock_pool)

    async def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            _DeltaRepository(transaction_repository=None)
        with self.assertRaises(NotProvidedException):
            # noinspection PyArgumentEqualDefault
            _DeltaRepository(lock_pool=None, pool_factory=None)

    def test_transaction(self):
        uuid = uuid4()
        transaction = self.delta_repository.transaction(uuid=uuid)
        self.assertEqual(TransactionEntry(uuid), transaction)
        self.assertEqual(self.transaction_repository, transaction.repository)

    async def test_create(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.delta_repository.submit = mock

        entry = DeltaEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.delta_repository.create(entry))

        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_update(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.delta_repository.submit = mock

        entry = DeltaEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.delta_repository.update(entry))

        self.assertEqual(Action.UPDATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_delete(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.delta_repository.submit = mock

        entry = DeltaEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.delta_repository.delete(entry))

        self.assertEqual(Action.DELETE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_submit(self):
        created_at = current_datetime()
        id_ = 12
        field_diff_container = FieldDiffContainer([FieldDiff("color", str, "red")])

        async def _fn(e: DeltaEntry) -> DeltaEntry:
            e.id = id_
            e.version = 56
            e.created_at = created_at
            return e

        submit_mock = AsyncMock(side_effect=_fn)
        self.delta_repository._submit = submit_mock

        uuid = uuid4()
        delta = Delta(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.delta_repository.validate = validate_mock

        observed = await self.delta_repository.submit(delta)

        self.assertIsInstance(observed, DeltaEntry)
        self.assertEqual(uuid, observed.uuid)
        self.assertEqual("example.Car", observed.name)
        self.assertEqual(56, observed.version)
        self.assertEqual(field_diff_container, FieldDiffContainer.from_avro_bytes(observed.data))
        self.assertEqual(12, observed.id)
        self.assertEqual(Action.UPDATE, observed.action)
        self.assertEqual(created_at, observed.created_at)
        self.assertEqual(NULL_UUID, observed.transaction_uuid)

    async def test_submit_in_transaction(self):
        created_at = current_datetime()
        id_ = 12
        field_diff_container = FieldDiffContainer([FieldDiff("color", str, "red")])
        transaction = TransactionEntry(uuid4())

        TRANSACTION_CONTEXT_VAR.set(transaction)

        async def _fn(e: DeltaEntry) -> DeltaEntry:
            e.id = id_
            e.version = 56
            e.created_at = created_at
            return e

        submit_mock = AsyncMock(side_effect=_fn)
        self.delta_repository._submit = submit_mock

        uuid = uuid4()
        delta = Delta(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.delta_repository.validate = validate_mock

        observed = await self.delta_repository.submit(delta)

        self.assertIsInstance(observed, DeltaEntry)
        self.assertEqual(uuid, observed.uuid)
        self.assertEqual("example.Car", observed.name)
        self.assertEqual(56, observed.version)
        self.assertEqual(field_diff_container, FieldDiffContainer.from_avro_bytes(observed.data))
        self.assertEqual(12, observed.id)
        self.assertEqual(Action.UPDATE, observed.action)
        self.assertEqual(created_at, observed.created_at)
        self.assertEqual(transaction.uuid, observed.transaction_uuid)

    async def test_submit_raises_missing_action(self):
        entry = DeltaEntry(uuid4(), "example.Car", 0, bytes())
        with self.assertRaises(DeltaRepositoryException):
            await self.delta_repository.submit(entry)

    async def test_submit_raises_conflict(self):
        validate_mock = AsyncMock(return_value=False)

        self.delta_repository.validate = validate_mock

        entry = DeltaEntry(uuid4(), "example.Car", 0, bytes(), action=Action.CREATE)
        with self.assertRaises(DeltaRepositoryConflictException):
            await self.delta_repository.submit(entry)

    async def test_submit_context_var(self):
        mocked_delta = AsyncMock()
        mocked_delta.action = Action.CREATE

        async def _fn(entry):
            self.assertEqual(True, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())
            return entry

        mocked_submit = AsyncMock(side_effect=_fn)
        self.delta_repository._submit = mocked_submit

        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())
        await self.delta_repository.submit(mocked_delta)
        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())

        self.assertEqual(1, mocked_submit.call_count)

    async def test_validate_true(self):
        uuid = uuid4()
        transaction_uuid = uuid4()

        deltas = []
        transactions = [TransactionEntry(transaction_uuid, TransactionStatus.RESERVING)]

        select_delta_mock = MagicMock(return_value=FakeAsyncIterator(deltas))
        self.delta_repository.select = select_delta_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = DeltaEntry(uuid, "example.Car", 1, bytes(), 1, Action.CREATE, datetime.now(), transaction_uuid)

        self.assertTrue(await self.delta_repository.validate(entry))

        self.assertEqual(
            [call(uuid=uuid, transaction_uuid_in=(transaction_uuid,))],
            select_delta_mock.call_args_list,
        )

        self.assertEqual(
            [
                call(
                    destination_uuid=NULL_UUID,
                    uuid_ne=None,
                    status_in=(TransactionStatus.RESERVING, TransactionStatus.RESERVED, TransactionStatus.COMMITTING),
                )
            ],
            select_transaction_mock.call_args_list,
        )

    async def test_validate_refs(self):
        uuid = uuid4()
        transaction_uuid = uuid4()

        deltas = []
        transactions = [TransactionEntry(transaction_uuid, TransactionStatus.RESERVING)]

        select_delta_mock = MagicMock(return_value=FakeAsyncIterator(deltas))
        self.delta_repository.select = select_delta_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        # entry = DeltaEntry(uuid, "example.Car", 1, bytes(), 1, Action.CREATE, datetime.now(), transaction_uuid)
        entry = DeltaEntry(uuid, "example.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=transaction_uuid)


        self.assertTrue(await self.delta_repository.validate(entry))

        self.assertEqual(
            [call(uuid=uuid, transaction_uuid_in=(transaction_uuid,))],
            select_delta_mock.call_args_list,
        )

        self.assertEqual(
            [
                call(
                    destination_uuid=NULL_UUID,
                    uuid_ne=None,
                    status_in=(TransactionStatus.RESERVING, TransactionStatus.RESERVED, TransactionStatus.COMMITTING),
                )
            ],
            select_transaction_mock.call_args_list,
        )

    async def test_validate_with_skip(self):
        uuid = uuid4()
        transaction_uuid = uuid4()
        another_transaction_uuid = uuid4()

        deltas = []
        transactions = [TransactionEntry(another_transaction_uuid, TransactionStatus.RESERVING)]

        select_delta_mock = MagicMock(return_value=FakeAsyncIterator(deltas))
        self.delta_repository.select = select_delta_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = DeltaEntry(uuid, "example.Car")
        self.assertTrue(await self.delta_repository.validate(entry, transaction_uuid_ne=transaction_uuid))

        self.assertEqual(
            [
                call(
                    destination_uuid=NULL_UUID,
                    uuid_ne=transaction_uuid,
                    status_in=(TransactionStatus.RESERVING, TransactionStatus.RESERVED, TransactionStatus.COMMITTING),
                )
            ],
            select_transaction_mock.call_args_list,
        )

        self.assertEqual(
            [call(uuid=uuid, transaction_uuid_in=(another_transaction_uuid,))],
            select_delta_mock.call_args_list,
        )

    async def test_validate_false(self):
        uuid = uuid4()
        transaction_uuid = uuid4()

        deltas = [
            DeltaEntry(uuid, "example.Car", 1),
            DeltaEntry(uuid, "example.Car", 2, transaction_uuid=transaction_uuid),
        ]
        transactions = [TransactionEntry(transaction_uuid, TransactionStatus.RESERVED)]

        select_delta_mock = MagicMock(return_value=FakeAsyncIterator(deltas))
        self.delta_repository.select = select_delta_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = DeltaEntry(uuid, "example.Car")

        self.assertFalse(await self.delta_repository.validate(entry))

        self.assertEqual(
            [call(uuid=uuid, transaction_uuid_in=(transaction_uuid,))],
            select_delta_mock.call_args_list,
        )

        self.assertEqual(
            [
                call(
                    destination_uuid=NULL_UUID,
                    uuid_ne=None,
                    status_in=(TransactionStatus.RESERVING, TransactionStatus.RESERVED, TransactionStatus.COMMITTING),
                )
            ],
            select_transaction_mock.call_args_list,
        )

    def test_write_lock(self):
        expected = FakeLock()
        mock = MagicMock(return_value=expected)

        self.pool_factory.get_pool("lock").acquire = mock

        self.assertEqual(expected, self.delta_repository.write_lock())
        self.assertEqual([call("aggregate_delta_write_lock")], mock.call_args_list)

    async def test_select(self):
        mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.delta_repository._select = mock

        uuid = uuid4()

        transaction_uuid = uuid4()
        iterable = self.delta_repository.select(uuid=uuid, name=Car, id_gt=56, transaction_uuid=transaction_uuid)
        observed = [a async for a in iterable]
        self.assertEqual(list(range(5)), observed)

        self.assertEqual(1, mock.call_count)
        args = call(
            uuid=uuid,
            name=classname(Car),
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
            transaction_uuid_in=None,
        )

        self.assertEqual(args, mock.call_args)

    async def test_offset(self):
        with patch(
            f"{__name__}._DeltaRepository._offset", new_callable=PropertyMock, side_effect=AsyncMock(return_value=56)
        ) as mock:
            self.assertEqual(56, await self.delta_repository.offset)
            self.assertEqual(1, mock.call_count)

    async def test_get_collided_transactions(self):
        uuid = uuid4()
        another = uuid4()

        agg_uuid = uuid4()

        select_delta_1 = [
            DeltaEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=uuid),
            DeltaEntry(agg_uuid, "c.Car", 3, bytes(), 2, Action.UPDATE, transaction_uuid=uuid),
            DeltaEntry(agg_uuid, "c.Car", 2, bytes(), 3, Action.UPDATE, transaction_uuid=uuid),
        ]

        select_delta_2 = [
            DeltaEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=uuid),
            DeltaEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=another),
        ]

        select_delta_mock = MagicMock(
            side_effect=[FakeAsyncIterator(select_delta_1), FakeAsyncIterator(select_delta_2)]
        )
        self.delta_repository.select = select_delta_mock

        expected = {another}
        observed = await self.delta_repository.get_collided_transactions(uuid)
        self.assertEqual(expected, observed)

        self.assertEqual(
            [call(transaction_uuid=uuid), call(uuid=agg_uuid, version=3)], select_delta_mock.call_args_list
        )

    async def test_commit(self) -> None:
        uuid = uuid4()

        agg_uuid = uuid4()

        async def _fn(*args, **kwargs):
            yield DeltaEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=uuid)
            yield DeltaEntry(agg_uuid, "c.Car", 3, bytes(), 2, Action.UPDATE, transaction_uuid=uuid)
            yield DeltaEntry(agg_uuid, "c.Car", 2, bytes(), 3, Action.UPDATE, transaction_uuid=uuid)

        select_mock = MagicMock(side_effect=_fn)
        submit_mock = AsyncMock()

        self.delta_repository.select = select_mock
        self.delta_repository.submit = submit_mock

        with patch.object(DeltaRepository, "offset", new_callable=PropertyMock, side_effect=AsyncMock(return_value=55)):
            await self.delta_repository.commit_transaction(uuid, NULL_UUID)

        self.assertEqual(
            [
                call(DeltaEntry(agg_uuid, "c.Car", 1, bytes(), action=Action.CREATE), transaction_uuid_ne=uuid),
                call(DeltaEntry(agg_uuid, "c.Car", 3, bytes(), action=Action.UPDATE), transaction_uuid_ne=uuid),
                call(DeltaEntry(agg_uuid, "c.Car", 2, bytes(), action=Action.UPDATE), transaction_uuid_ne=uuid),
            ],
            submit_mock.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()
