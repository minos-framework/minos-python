from abc import (
    ABC,
    abstractmethod,
)
from datetime import (
    datetime,
)
from typing import (
    Optional,
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
    EventEntry,
    FieldDiff,
    FieldDiffContainer,
    NotFoundException,
    Ordering,
    Ref,
    RootEntity,
    SnapshotEntry,
    SnapshotRepository,
    TransactionEntry,
    TransactionStatus,
)
from minos.common import (
    current_datetime,
)
from minos.common.testing import (
    MinosTestCase,
)


class Owner(RootEntity):
    """For testing purposes"""

    name: str
    surname: str
    age: Optional[int]


class Car(RootEntity):
    """For testing purposes"""

    doors: int
    color: str
    owner: Optional[Ref[Owner]]


class SnapshotRepositoryTestCase(MinosTestCase, ABC):
    snapshot_repository: SnapshotRepository

    def setUp(self) -> None:
        super().setUp()
        self.snapshot_repository = self.build_snapshot_repository()

    @abstractmethod
    def build_snapshot_repository(self) -> SnapshotRepository:
        pass

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.snapshot_repository.setup()

    async def asyncTearDown(self):
        await self.snapshot_repository.destroy()
        await super().asyncTearDown()

    def assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
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


class SnapshotRepositoryWriterTestCase(SnapshotRepositoryTestCase, ABC):
    __test__ = False

    def setUp(self) -> None:
        super().setUp()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()

        self.transaction_1 = uuid4()
        self.transaction_2 = uuid4()
        self.transaction_3 = uuid4()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self._populate()

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
        self.assertTrue(isinstance(self.snapshot_repository, SnapshotRepository))

    async def test_dispatch(self):
        await self.snapshot_repository.synchronize()

        # noinspection PyTypeChecker
        iterable = self.snapshot_repository.find_entries(
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
        self.assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_first_transaction(self):
        await self.snapshot_repository.synchronize()

        # noinspection PyTypeChecker
        iterable = self.snapshot_repository.find_entries(
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
        self.assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_second_transaction(self):
        await self.snapshot_repository.synchronize()

        # noinspection PyTypeChecker
        iterable = self.snapshot_repository.find_entries(
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
        self.assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_third_transaction(self):
        await self.snapshot_repository.synchronize()

        # noinspection PyTypeChecker
        iterable = self.snapshot_repository.find_entries(
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
        self.assert_equal_snapshot_entries(expected, observed)

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
        await self.snapshot_repository.synchronize()

        observed = [v async for v in self.snapshot_repository.find_entries(name, condition)]

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
        self.assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_with_offset(self):
        mock = MagicMock(side_effect=self.event_repository.select)
        self.event_repository.select = mock

        await self.snapshot_repository.synchronize()
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

        await self.snapshot_repository.synchronize()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(id_gt=11), mock.call_args)
        mock.reset_mock()

        await self.snapshot_repository.synchronize()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(id_gt=12), mock.call_args)
        mock.reset_mock()

        await self.snapshot_repository.synchronize()
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(id_gt=12), mock.call_args)
        mock.reset_mock()


class SnapshotRepositoryReaderTestCase(SnapshotRepositoryTestCase, ABC):
    __test__ = False

    def setUp(self) -> None:
        super().setUp()

        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()

        self.transaction_1 = uuid4()
        self.transaction_2 = uuid4()
        self.transaction_3 = uuid4()
        self.transaction_4 = uuid4()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self._populate()

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
        await self.transaction_repository.submit(
            TransactionEntry(
                self.transaction_4, TransactionStatus.REJECTED, await self.event_repository.offset, self.transaction_3
            )
        )
        await self.snapshot_repository.synchronize()

    async def test_find_by_uuid(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])

        iterable = self.snapshot_repository.find(Car, condition, ordering=Ordering.ASC("updated_at"))
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_2,
                version=2,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[1].created_at,
                updated_at=observed[1].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_find_with_transaction(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])

        iterable = self.snapshot_repository.find(
            Car,
            condition,
            ordering=Ordering.ASC("updated_at"),
            transaction=TransactionEntry(self.transaction_1),
        )
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_2,
                version=4,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[1].created_at,
                updated_at=observed[1].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_find_with_transaction_delete(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])

        iterable = self.snapshot_repository.find(
            Car,
            condition,
            ordering=Ordering.ASC("updated_at"),
            transaction=TransactionEntry(self.transaction_2),
        )
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_find_with_transaction_reverted(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])

        iterable = self.snapshot_repository.find(
            Car, condition, ordering=Ordering.ASC("updated_at"), transaction_uuid=self.transaction_3
        )
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_2,
                version=2,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[1].created_at,
                updated_at=observed[1].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_find_streaming_true(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])

        iterable = self.snapshot_repository.find(
            Car, condition, streaming_mode=True, ordering=Ordering.ASC("updated_at")
        )
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_2,
                version=2,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[1].created_at,
                updated_at=observed[1].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_find_with_duplicates(self):
        uuids = [self.uuid_2, self.uuid_2, self.uuid_3]
        condition = Condition.IN("uuid", uuids)

        iterable = self.snapshot_repository.find(Car, condition, ordering=Ordering.ASC("updated_at"))
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_2,
                version=2,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[1].created_at,
                updated_at=observed[1].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_find_empty(self):

        observed = {v async for v in self.snapshot_repository.find(Car, Condition.FALSE)}

        expected = set()
        self.assertEqual(expected, observed)

    async def test_get(self):

        observed = await self.snapshot_repository.get(Car, self.uuid_2)

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=2, created_at=observed.created_at, updated_at=observed.updated_at
        )
        self.assertEqual(expected, observed)

    async def test_get_with_transaction(self):

        observed = await self.snapshot_repository.get(
            Car, self.uuid_2, transaction=TransactionEntry(self.transaction_1)
        )

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=4, created_at=observed.created_at, updated_at=observed.updated_at
        )
        self.assertEqual(expected, observed)

    async def test_get_raises(self):

        with self.assertRaises(AlreadyDeletedException):
            await self.snapshot_repository.get(Car, self.uuid_1)
        with self.assertRaises(NotFoundException):
            await self.snapshot_repository.get(Car, uuid4())

    async def test_get_with_transaction_raises(self):

        with self.assertRaises(AlreadyDeletedException):
            await self.snapshot_repository.get(Car, self.uuid_2, transaction=TransactionEntry(self.transaction_2))

    async def test_find(self):
        condition = Condition.EQUAL("color", "blue")
        iterable = self.snapshot_repository.find(Car, condition, ordering=Ordering.ASC("updated_at"))
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_2,
                version=2,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[1].created_at,
                updated_at=observed[1].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_find_all(self):

        iterable = self.snapshot_repository.find(Car, Condition.TRUE, Ordering.ASC("updated_at"))
        observed = [v async for v in iterable]

        expected = [
            Car(
                3,
                "blue",
                uuid=self.uuid_2,
                version=2,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            ),
            Car(
                3,
                "blue",
                uuid=self.uuid_3,
                version=1,
                created_at=observed[1].created_at,
                updated_at=observed[1].updated_at,
            ),
        ]
        self.assertEqual(expected, observed)

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