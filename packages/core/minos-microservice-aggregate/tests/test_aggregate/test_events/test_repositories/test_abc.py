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
    Event,
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
    NotProvidedException,
    SetupMixin,
    classname,
    current_datetime,
)
from minos.networks import (
    BrokerMessageV1,
)
from tests.utils import (
    AggregateTestCase,
    Car,
    FakeAsyncIterator,
    FakeLock,
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


class TestEventRepository(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.event_repository = _EventRepository()

    def test_subclass(self):
        self.assertTrue(issubclass(EventRepository, (ABC, SetupMixin)))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select", "_offset"}, EventRepository.__abstractmethods__)

    def test_constructor(self):
        repository = _EventRepository()
        self.assertEqual(self.broker_publisher, repository._broker_publisher)
        self.assertEqual(self.transaction_repository, repository.transaction_repository)
        self.assertEqual(self.pool_factory.get_pool("lock"), repository._lock_pool)

    async def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            _EventRepository(broker_publisher=None)
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            _EventRepository(transaction_repository=None)
        with self.assertRaises(NotProvidedException):
            # noinspection PyArgumentEqualDefault
            _EventRepository(lock_pool=None, pool_factory=None)

    def test_transaction(self):
        uuid = uuid4()
        transaction = self.event_repository.transaction(uuid=uuid)
        self.assertEqual(TransactionEntry(uuid), transaction)
        self.assertEqual(self.transaction_repository, transaction.repository)

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
        event = Event(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.event_repository.validate = validate_mock

        observed = await self.event_repository.submit(event)

        self.assertEqual(1, send_events_mock.call_count)

        self.assertIsInstance(observed, EventEntry)
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
        event = Event(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.event_repository.validate = validate_mock

        observed = await self.event_repository.submit(event)

        self.assertEqual(0, send_events_mock.call_count)

        self.assertIsInstance(observed, EventEntry)
        self.assertEqual(uuid, observed.uuid)
        self.assertEqual("example.Car", observed.name)
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
        event = Event(
            uuid=uuid,
            name="example.Car",
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=field_diff_container,
        )

        validate_mock = AsyncMock(return_value=True)
        self.event_repository.validate = validate_mock

        await self.event_repository.submit(event)

        observed = self.broker_publisher.messages

        self.assertEqual(2, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarUpdated", observed[0].topic)
        self.assertEqual(
            Event(
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
            Event(
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
        event = EventEntry.from_event(
            Event(
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

        await self.event_repository.submit(event)

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
        mocked_event = AsyncMock()
        mocked_event.action = Action.CREATE

        async def _fn(entry):
            self.assertEqual(True, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())
            return entry

        mocked_submit = AsyncMock(side_effect=_fn)
        self.event_repository._submit = mocked_submit

        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())
        await self.event_repository.submit(mocked_event)
        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())

        self.assertEqual(1, mocked_submit.call_count)

    async def test_validate_true(self):
        uuid = uuid4()
        transaction_uuid = uuid4()

        events = []
        transactions = [TransactionEntry(transaction_uuid, TransactionStatus.RESERVING)]

        select_event_mock = MagicMock(return_value=FakeAsyncIterator(events))
        self.event_repository.select = select_event_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = EventEntry(uuid, "example.Car")

        self.assertTrue(await self.event_repository.validate(entry))

        self.assertEqual(
            [call(uuid=uuid, transaction_uuid_in=(transaction_uuid,))],
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
        uuid = uuid4()
        transaction_uuid = uuid4()
        another_transaction_uuid = uuid4()

        events = []
        transactions = [TransactionEntry(another_transaction_uuid, TransactionStatus.RESERVING)]

        select_event_mock = MagicMock(return_value=FakeAsyncIterator(events))
        self.event_repository.select = select_event_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = EventEntry(uuid, "example.Car")
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
            [call(uuid=uuid, transaction_uuid_in=(another_transaction_uuid,))],
            select_event_mock.call_args_list,
        )

    async def test_validate_false(self):
        uuid = uuid4()
        transaction_uuid = uuid4()

        events = [
            EventEntry(uuid, "example.Car", 1),
            EventEntry(uuid, "example.Car", 2, transaction_uuid=transaction_uuid),
        ]
        transactions = [TransactionEntry(transaction_uuid, TransactionStatus.RESERVED)]

        select_event_mock = MagicMock(return_value=FakeAsyncIterator(events))
        self.event_repository.select = select_event_mock

        select_transaction_mock = MagicMock(return_value=FakeAsyncIterator(transactions))
        self.transaction_repository.select = select_transaction_mock

        entry = EventEntry(uuid, "example.Car")

        self.assertFalse(await self.event_repository.validate(entry))

        self.assertEqual(
            [call(uuid=uuid, transaction_uuid_in=(transaction_uuid,))],
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

        self.pool_factory.get_pool("lock").acquire = mock

        self.assertEqual(expected, self.event_repository.write_lock())
        self.assertEqual([call("aggregate_event_write_lock")], mock.call_args_list)

    async def test_select(self):
        mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.event_repository._select = mock

        uuid = uuid4()

        transaction_uuid = uuid4()
        iterable = self.event_repository.select(uuid=uuid, name=Car, id_gt=56, transaction_uuid=transaction_uuid)
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
            f"{__name__}._EventRepository._offset", new_callable=PropertyMock, side_effect=AsyncMock(return_value=56)
        ) as mock:
            self.assertEqual(56, await self.event_repository.offset)
            self.assertEqual(1, mock.call_count)

    async def test_get_related_transactions(self):
        uuid = uuid4()
        another = uuid4()

        agg_uuid = uuid4()

        select_event_1 = [
            EventEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=uuid),
            EventEntry(agg_uuid, "c.Car", 3, bytes(), 2, Action.UPDATE, transaction_uuid=uuid),
            EventEntry(agg_uuid, "c.Car", 2, bytes(), 3, Action.UPDATE, transaction_uuid=uuid),
        ]

        select_event_2 = [
            EventEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=uuid),
            EventEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=another),
        ]

        select_event_mock = MagicMock(
            side_effect=[FakeAsyncIterator(select_event_1), FakeAsyncIterator(select_event_2)]
        )
        self.event_repository.select = select_event_mock

        expected = {another}
        observed = await self.event_repository.get_related_transactions(uuid)
        self.assertEqual(expected, observed)

        self.assertEqual(
            [call(transaction_uuid=uuid), call(uuid=agg_uuid, version=3)], select_event_mock.call_args_list
        )

    async def test_commit(self) -> None:
        uuid = uuid4()

        agg_uuid = uuid4()

        async def _fn(*args, **kwargs):
            yield EventEntry(agg_uuid, "c.Car", 1, bytes(), 1, Action.CREATE, transaction_uuid=uuid)
            yield EventEntry(agg_uuid, "c.Car", 3, bytes(), 2, Action.UPDATE, transaction_uuid=uuid)
            yield EventEntry(agg_uuid, "c.Car", 2, bytes(), 3, Action.UPDATE, transaction_uuid=uuid)

        select_mock = MagicMock(side_effect=_fn)
        submit_mock = AsyncMock()

        self.event_repository.select = select_mock
        self.event_repository.submit = submit_mock

        with patch.object(EventRepository, "offset", new_callable=PropertyMock, side_effect=AsyncMock(return_value=55)):
            await self.event_repository.commit_transaction(uuid, NULL_UUID)

        self.assertEqual(
            [
                call(EventEntry(agg_uuid, "c.Car", 1, bytes(), action=Action.CREATE), transaction_uuid_ne=uuid),
                call(EventEntry(agg_uuid, "c.Car", 3, bytes(), action=Action.UPDATE), transaction_uuid_ne=uuid),
                call(EventEntry(agg_uuid, "c.Car", 2, bytes(), action=Action.UPDATE), transaction_uuid_ne=uuid),
            ],
            submit_mock.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()
