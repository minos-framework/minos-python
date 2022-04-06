import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    AlreadyDeletedException,
    Condition,
    DatabaseSnapshotReader,
    DatabaseSnapshotSetup,
    DatabaseSnapshotWriter,
    EventEntry,
    FieldDiff,
    FieldDiffContainer,
    Ordering,
    SnapshotEntry,
    TransactionEntry,
    TransactionStatus,
)
from minos.common import (
    DatabaseClientPool,
    NotProvidedException,
    current_datetime,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    AggregateTestCase,
    Car,
)


class TestPostgreSqlSnapshotWriter(AggregateTestCase, PostgresAsyncTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()

        self.transaction_1 = uuid4()
        self.transaction_2 = uuid4()
        self.transaction_3 = uuid4()

        self.reader = DatabaseSnapshotReader.from_config(self.config)
        self.writer = DatabaseSnapshotWriter.from_config(self.config, reader=self.reader)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.writer.setup()
        await self.reader.setup()
        await self._populate()

    async def asyncTearDown(self):
        await self.reader.destroy()
        await self.writer.destroy()
        await super().asyncTearDown()

    async def _populate(self):
        diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        # noinspection PyTypeChecker
        name: str = Car.classname

        await self.event_repository.create(EventEntry(self.uuid_1, name, 1, diff.avro_bytes))
        await self.event_repository.update(EventEntry(self.uuid_1, name, 2, diff.avro_bytes))
        await self.event_repository.create(EventEntry(self.uuid_2, name, 1, diff.avro_bytes))
        await self.event_repository.update(EventEntry(self.uuid_1, name, 3, diff.avro_bytes))
        await self.event_repository.delete(EventEntry(self.uuid_1, name, 4))
        await self.event_repository.update(EventEntry(self.uuid_2, name, 2, diff.avro_bytes))
        await self.event_repository.update(
            EventEntry(self.uuid_2, name, 3, diff.avro_bytes, transaction_uuid=self.transaction_1)
        )
        await self.event_repository.delete(
            EventEntry(self.uuid_2, name, 3, bytes(), transaction_uuid=self.transaction_2)
        )
        await self.event_repository.update(
            EventEntry(self.uuid_2, name, 4, diff.avro_bytes, transaction_uuid=self.transaction_1)
        )
        await self.event_repository.create(EventEntry(self.uuid_3, name, 1, diff.avro_bytes))
        await self.event_repository.delete(
            EventEntry(self.uuid_2, name, 3, bytes(), transaction_uuid=self.transaction_3)
        )
        await self.transaction_repository.submit(
            TransactionEntry(self.transaction_1, TransactionStatus.PENDING, await self.event_repository.offset)
        )
        await self.transaction_repository.submit(
            TransactionEntry(self.transaction_2, TransactionStatus.PENDING, await self.event_repository.offset)
        )
        await self.transaction_repository.submit(
            TransactionEntry(self.transaction_3, TransactionStatus.REJECTED, await self.event_repository.offset)
        )

    def test_type(self):
        self.assertTrue(issubclass(DatabaseSnapshotWriter, DatabaseSnapshotSetup))

    def test_from_config(self):
        self.assertIsInstance(self.writer.pool, DatabaseClientPool)

    def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            DatabaseSnapshotWriter.from_config(self.config, reader=self.reader, event_repository=None)

        with self.assertRaises(NotProvidedException):
            DatabaseSnapshotWriter.from_config(self.config, reader=self.reader, transaction_repository=None)

    async def test_dispatch(self):
        await self.writer.dispatch()

        # noinspection PyTypeChecker
        iterable = self.reader.find_entries(
            Car.classname, Condition.TRUE, Ordering.ASC("updated_at"), exclude_deleted=False
        )
        observed = [v async for v in iterable]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(self.uuid_1, Car.classname, 4),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=2,
                    created_at=observed[1].created_at,
                    updated_at=observed[1].updated_at,
                )
            ),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=observed[2].created_at,
                    updated_at=observed[2].updated_at,
                )
            ),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_first_transaction(self):
        await self.writer.dispatch()

        # noinspection PyTypeChecker
        iterable = self.reader.find_entries(
            Car.classname,
            Condition.TRUE,
            Ordering.ASC("updated_at"),
            exclude_deleted=False,
            transaction=TransactionEntry(self.transaction_1),
        )
        observed = [v async for v in iterable]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(self.uuid_1, Car.classname, 4),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=4,
                    created_at=observed[1].created_at,
                    updated_at=observed[1].updated_at,
                )
            ),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=observed[2].created_at,
                    updated_at=observed[2].updated_at,
                )
            ),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_second_transaction(self):
        await self.writer.dispatch()

        # noinspection PyTypeChecker
        iterable = self.reader.find_entries(
            Car.classname,
            Condition.TRUE,
            Ordering.ASC("updated_at"),
            exclude_deleted=False,
            transaction=TransactionEntry(self.transaction_2),
        )
        observed = [v async for v in iterable]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(self.uuid_1, Car.classname, 4),
            SnapshotEntry(self.uuid_2, Car.classname, 4),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=observed[2].created_at,
                    updated_at=observed[2].updated_at,
                )
            ),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_third_transaction(self):
        await self.writer.dispatch()

        # noinspection PyTypeChecker
        iterable = self.reader.find_entries(
            Car.classname,
            Condition.TRUE,
            Ordering.ASC("updated_at"),
            exclude_deleted=False,
            transaction_uuid=self.transaction_3,
        )
        observed = [v async for v in iterable]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(self.uuid_1, Car.classname, 4),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=2,
                    created_at=observed[1].created_at,
                    updated_at=observed[1].updated_at,
                )
            ),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=observed[2].created_at,
                    updated_at=observed[2].updated_at,
                )
            ),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    async def test_is_synced(self):
        self.assertFalse(await self.writer.is_synced("tests.utils.Car"))
        await self.writer.dispatch()
        self.assertTrue(await self.writer.is_synced("tests.utils.Car"))

    async def test_dispatch_ignore_previous_version(self):
        diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        # noinspection PyTypeChecker
        name: str = Car.classname
        condition = Condition.EQUAL("uuid", self.uuid_1)

        async def _fn(*args, **kwargs):
            yield EventEntry(self.uuid_1, name, 1, diff.avro_bytes, 1, Action.CREATE, current_datetime())
            yield EventEntry(self.uuid_1, name, 3, diff.avro_bytes, 2, Action.CREATE, current_datetime())
            yield EventEntry(self.uuid_1, name, 2, diff.avro_bytes, 3, Action.CREATE, current_datetime())

        self.event_repository.select = MagicMock(side_effect=_fn)
        await self.writer.dispatch()

        observed = [v async for v in self.reader.find_entries(name, condition)]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(
                uuid=self.uuid_1,
                name=name,
                version=3,
                schema=Car.avro_schema,
                data=Car(3, "blue", uuid=self.uuid_1, version=1).avro_data,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            )
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    def _assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            if exp.data is None:
                with self.assertRaises(AlreadyDeletedException):
                    # noinspection PyStatementEffect
                    obs.build()
            else:
                self.assertEqual(exp.build(), obs.build())
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def test_dispatch_with_offset(self):
        mock = MagicMock(side_effect=self.writer._event_repository.select)
        self.writer._event_repository.select = mock

        await self.writer.dispatch()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(id_gt=0), mock.call_args)
        mock.reset_mock()

        # noinspection PyTypeChecker
        entry = EventEntry(
            uuid=self.uuid_3,
            name=Car.classname,
            data=FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")]).avro_bytes,
        )
        await self.event_repository.create(entry)

        await self.writer.dispatch()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(id_gt=11), mock.call_args)
        mock.reset_mock()

        await self.writer.dispatch()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(id_gt=12), mock.call_args)
        mock.reset_mock()

        await self.writer.dispatch()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(id_gt=12), mock.call_args)
        mock.reset_mock()


if __name__ == "__main__":
    unittest.main()
