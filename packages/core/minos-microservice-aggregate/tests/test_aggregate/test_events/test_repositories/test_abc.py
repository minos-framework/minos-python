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
    TRANSACTION_CONTEXT_VAR,
    Action,
    AggregateDiff,
    EventEntry,
    EventRepository,
    EventRepositoryConflictException,
    EventRepositoryException,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
    TransactionEntry,
    TransactionStatus,
)
from minos.common import (
    NULL_UUID,
    MinosSetup,
    NotProvidedException,
    current_datetime,
)
from minos.networks import (
    BrokerMessageV1,
)
from tests.utils import (
    FakeAsyncIterator,
    FakeLock,
    MinosTestCase,
)


class _EventRepository(EventRepository):
    """For testing purposes."""

    async def _submit(self, entry: EventEntry, **kwargs) -> EventEntry:
        """For testing purposes."""

    def _select(self, *args, **kwargs) -> AsyncIterator[EventEntry]:
        """For testing purposes."""

    @property
    async def _offset(self) -> int:
        """For testing purposes."""
        return 0


class TestEventRepository(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.event_repository = _EventRepository()

    def test_subclass(self):
        self.assertTrue(issubclass(EventRepository, (ABC, MinosSetup)))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select", "_offset"}, EventRepository.__abstractmethods__)

    def test_constructor(self):
        repository = _EventRepository()
        self.assertEqual(self.broker_publisher, repository._broker_publisher)
        self.assertEqual(self.transaction_repository, repository._transaction_repository)
        self.assertEqual(self.lock_pool, repository._lock_pool)

    async def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            _EventRepository(broker_publisher=None)
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            _EventRepository(transaction_repository=None)
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            _EventRepository(lock_pool=None)

    def test_transaction(self):
        uuid = uuid4()
        transaction = self.event_repository.transaction(uuid=uuid)
        self.assertEqual(TransactionEntry(uuid), transaction)
        self.assertEqual(self.event_repository, transaction._event_repository)
        self.assertEqual(self.transaction_repository, transaction._transaction_repository)

    async def test_create(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.event_repository.submit = mock

        entry = EventEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.event_repository.create(entry))

        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_update(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.event_repository.submit = mock

        entry = EventEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.event_repository.update(entry))

        self.assertEqual(Action.UPDATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_delete(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.event_repository.submit = mock

        entry = EventEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.event_repository.delete(entry))

        self.assertEqual(Action.DELETE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_submit(self):
        created_at = current_datetime()
        id_ = 12
        field_diff_container = FieldDiffContainer([FieldDiff("color", str, "red")])

        async def _fn(e: EventEntry) -> EventEntry:
            e.id = id_
            e.version = 56
            e.created_at = created_at
            return e

        submit_mock = AsyncMock(side_effect=_fn)
        send_events_mock = AsyncMock()
        self.event_repository._submit = submit_mock
        self.event_repository._send_events = send_events_mock

        uuid = uuid4()
        aggregate_diff = AggregateDiff(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.event_repository.validate = validate_mock

        observed = await self.event_repository.submit(aggregate_diff)

        self.assertEqual(1, send_events_mock.call_count)

        self.assertIsInstance(observed, EventEntry)
        self.assertEqual(uuid, observed.aggregate_uuid)
        self.assertEqual("example.Car", observed.aggregate_name)
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

        async def _fn(e: EventEntry) -> EventEntry:
            e.id = id_
            e.version = 56
            e.created_at = created_at
            return e

        submit_mock = AsyncMock(side_effect=_fn)
        send_events_mock = AsyncMock()
        self.event_repository._submit = submit_mock
        self.event_repository._send_events = send_events_mock

        uuid = uuid4()
        aggregate_diff = AggregateDiff(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.event_repository.validate = validate_mock

        observed = await self.event_repository.submit(aggregate_diff)

        self.assertEqual(0, send_events_mock.call_count)

        self.assertIsInstance(observed, EventEntry)
        self.assertEqual(uuid, observed.aggregate_uuid)
        self.assertEqual("example.Car", observed.aggregate_name)
        self.assertEqual(56, observed.version)
        self.assertEqual(field_diff_container, FieldDiffContainer.from_avro_bytes(observed.data))
        self.assertEqual(12, observed.id)
        self.assertEqual(Action.UPDATE, observed.action)
        self.assertEqual(created_at, observed.created_at)
        self.assertEqual(transaction.uuid, observed.transaction_uuid)

    async def test_submit_send_events(self):
        created_at = current_datetime()
        id_ = 12
        field_diff_container = FieldDiffContainer([IncrementalFieldDiff("colors", str, "red", Action.CREATE)])

        async def _fn(e: EventEntry) -> EventEntry:
            e.id = id_
            e.version = 56
            e.created_at = created_at
            return e

        submit_mock = AsyncMock(side_effect=_fn)
        self.event_repository._submit = submit_mock

        uuid = uuid4()
        aggregate_diff = AggregateDiff(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.event_repository.validate = validate_mock

        await self.event_repository.submit(aggregate_diff)

        observed = self.broker_publisher.messages

        self.assertEqual(2, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarUpdated", observed[0].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=uuid,
                name="example.Car",
                version=56,
                action=Action.UPDATE,
                created_at=created_at,
                fields_diff=field_diff_container,
            ),
            observed[0].content,
        )
        self.assertEqual("CarUpdated.colors.create", observed[1].topic)
        self.assertIsInstance(observed[1], BrokerMessageV1)
        self.assertEqual(
            AggregateDiff(
                uuid=uuid,
                name="example.Car",
                version=56,
                action=Action.UPDATE,
                created_at=created_at,
                fields_diff=field_diff_container,
            ),
            observed[1].content,
        )

    async def test_submit_not_send_events(self):
        created_at = current_datetime()
        id_ = 12
        field_diff_container = FieldDiffContainer([IncrementalFieldDiff("colors", str, "red", Action.CREATE)])

        async def _fn(e: EventEntry) -> EventEntry:
            e.id = id_
            e.version = 56
            e.created_at = created_at
            return e

        submit_mock = AsyncMock(side_effect=_fn)
        self.event_repository._submit = submit_mock

        uuid = uuid4()
        aggregate_diff = EventEntry.from_aggregate_diff(
            AggregateDiff(
                uuid=uuid,
                name="example.Car",
                version=2,
                action=Action.UPDATE,
                created_at=current_datetime(),
                fields_diff=field_diff_container,
            ),
            transaction_uuid=uuid4(),
        )

        validate_mock = AsyncMock(return_value=True)
        self.event_repository.validate = validate_mock

        await self.event_repository.submit(aggregate_diff)

        observed = self.broker_publisher.messages

        self.assertEqual(0, len(observed))

    async def test_submit_raises_missing_action(self):
        entry = EventEntry(uuid4(), "example.Car", 0, bytes())
        with self.assertRaises(EventRepositoryException):
            await self.event_repository.submit(entry)

    async def test_submit_raises_conflict(self):
        validate_mock = AsyncMock(return_value=False)

        self.event_repository.validate = validate_mock

        entry = EventEntry(uuid4(), "example.Car", 0, bytes(), action=Action.CREATE)
        with self.assertRaises(EventRepositoryConflictException):
            await self.event_repository.submit(entry)

    async def test_submit_context_var(self):
        mocked_aggregate_diff = AsyncMock()
        mocked_aggregate_diff.action = Action.CREATE

        async def _fn(entry):
            self.assertEqual(True, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())
            return entry

        mocked_submit = AsyncMock(side_effect=_fn)
        self.event_repository._submit = mocked_submit

        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())
        await self.event_repository.submit(mocked_aggregate_diff)
        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())

        self.assertEqual(1, mocked_submit.call_count)

    async def test_validate_true(self):
        aggregate_uuid = uuid4()
        transaction_uuid = uuid4()

        events = []
        transactions = [TransactionEntry(transaction_uuid, TransactionStatus.RESERVING)]

        select_event_mock = MagicMock(return_value=FakeAsyncIterator(events))
        self.event_repository.select = select_event_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = EventEntry(aggregate_uuid, "example.Car")

        self.assertTrue(await self.event_repository.validate(entry))

        self.assertEqual(
            [call(aggregate_uuid=aggregate_uuid, transaction_uuid_in=(transaction_uuid,))],
            select_event_mock.call_args_list,
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
        aggregate_uuid = uuid4()
        transaction_uuid = uuid4()
        another_transaction_uuid = uuid4()

        events = []
        transactions = [TransactionEntry(another_transaction_uuid, TransactionStatus.RESERVING)]

        select_event_mock = MagicMock(return_value=FakeAsyncIterator(events))
        self.event_repository.select = select_event_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = EventEntry(aggregate_uuid, "example.Car")
        self.assertTrue(await self.event_repository.validate(entry, transaction_uuid_ne=transaction_uuid))

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
            [call(aggregate_uuid=aggregate_uuid, transaction_uuid_in=(another_transaction_uuid,))],
            select_event_mock.call_args_list,
        )

    async def test_validate_false(self):
        aggregate_uuid = uuid4()
        transaction_uuid = uuid4()

        events = [
            EventEntry(aggregate_uuid, "example.Car", 1),
            EventEntry(aggregate_uuid, "example.Car", 2, transaction_uuid=transaction_uuid),
        ]
        transactions = [TransactionEntry(transaction_uuid, TransactionStatus.RESERVED)]

        select_event_mock = MagicMock(return_value=FakeAsyncIterator(events))
        self.event_repository.select = select_event_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = EventEntry(aggregate_uuid, "example.Car")

        self.assertFalse(await self.event_repository.validate(entry))

        self.assertEqual(
            [call(aggregate_uuid=aggregate_uuid, transaction_uuid_in=(transaction_uuid,))],
            select_event_mock.call_args_list,
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

        self.lock_pool.acquire = mock

        self.assertEqual(expected, self.event_repository.write_lock())
        self.assertEqual([call("aggregate_event_write_lock")], mock.call_args_list)

    async def test_select(self):
        mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.event_repository._select = mock

        aggregate_uuid = uuid4()
        aggregate_name = "path.to.Aggregate"

        transaction_uuid = uuid4()
        iterable = self.event_repository.select(
            aggregate_uuid=aggregate_uuid, aggregate_name=aggregate_name, id_gt=56, transaction_uuid=transaction_uuid
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
            transaction_uuid_in=None,
        )

        self.assertEqual(args, mock.call_args)

    async def test_offset(self):
        with patch(
            f"{__name__}._EventRepository._offset", new_callable=PropertyMock, side_effect=AsyncMock(return_value=56)
        ) as mock:
            self.assertEqual(56, await self.event_repository.offset)
            self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
