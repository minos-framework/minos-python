import sys
import unittest
from datetime import (
    datetime,
)
from uuid import (
    uuid4,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    Condition,
    FieldDiff,
    FieldDiffContainer,
    InMemoryRepository,
    InMemorySnapshot,
    MinosSnapshot,
    MinosSnapshotAggregateNotFoundException,
    MinosSnapshotDeletedAggregateException,
    Ordering,
    RepositoryEntry,
    SnapshotEntry,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
    FakeSnapshot,
    FakeTransactionRepository,
)


class TestMemorySnapshotReader(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()

        self.first_transaction = uuid4()
        self.second_transaction = uuid4()

        self.container = containers.DynamicContainer()
        self.container.repository = providers.Singleton(FakeRepository)
        self.container.snapshot = providers.Singleton(FakeSnapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()

    async def _populate(self):
        diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        # noinspection PyTypeChecker
        aggregate_name: str = Car.classname
        async with InMemoryRepository(
            event_broker=FakeBroker(), transaction_repository=FakeTransactionRepository()
        ) as repository:
            await repository.create(RepositoryEntry(self.uuid_1, aggregate_name, 1, diff.avro_bytes))
            await repository.update(RepositoryEntry(self.uuid_1, aggregate_name, 2, diff.avro_bytes))
            await repository.create(RepositoryEntry(self.uuid_2, aggregate_name, 1, diff.avro_bytes))
            await repository.update(RepositoryEntry(self.uuid_1, aggregate_name, 3, diff.avro_bytes))
            await repository.delete(RepositoryEntry(self.uuid_1, aggregate_name, 4))
            await repository.update(RepositoryEntry(self.uuid_2, aggregate_name, 2, diff.avro_bytes))
            await repository.update(
                RepositoryEntry(
                    self.uuid_2, aggregate_name, 3, diff.avro_bytes, transaction_uuid=self.first_transaction
                )
            )
            await repository.delete(
                RepositoryEntry(self.uuid_2, aggregate_name, 3, bytes(), transaction_uuid=self.second_transaction)
            )
            await repository.update(
                RepositoryEntry(
                    self.uuid_2, aggregate_name, 4, diff.avro_bytes, transaction_uuid=self.first_transaction
                )
            )
            await repository.create(RepositoryEntry(self.uuid_3, aggregate_name, 1, diff.avro_bytes))
            return InMemorySnapshot(repository=repository)

    def test_type(self):
        self.assertTrue(issubclass(InMemorySnapshot, MinosSnapshot))

    async def test_find_by_uuid(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])
        async with await self._populate() as snapshot:
            iterable = snapshot.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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
        async with await self._populate() as snapshot:
            iterable = snapshot.find(
                "tests.aggregate_classes.Car",
                condition,
                ordering=Ordering.ASC("updated_at"),
                transaction_uuid=self.first_transaction,
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
        async with await self._populate() as snapshot:
            iterable = snapshot.find(
                "tests.aggregate_classes.Car",
                condition,
                ordering=Ordering.ASC("updated_at"),
                transaction_uuid=self.second_transaction,
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

    async def test_find_streaming_true(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])

        async with await self._populate() as snapshot:
            iterable = snapshot.find(
                "tests.aggregate_classes.Car", condition, streaming_mode=True, ordering=Ordering.ASC("updated_at")
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
        async with await self._populate() as snapshot:
            iterable = snapshot.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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
        async with await self._populate() as snapshot:
            observed = {v async for v in snapshot.find("tests.aggregate_classes.Car", Condition.FALSE)}

        expected = set()
        self.assertEqual(expected, observed)

    async def test_get(self):
        async with await self._populate() as snapshot:
            observed = await snapshot.get("tests.aggregate_classes.Car", self.uuid_2)

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=2, created_at=observed.created_at, updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_get_with_transaction(self):
        async with await self._populate() as snapshot:
            observed = await snapshot.get(
                "tests.aggregate_classes.Car", self.uuid_2, transaction_uuid=self.first_transaction
            )

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=4, created_at=observed.created_at, updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_get_raises(self):
        async with await self._populate() as snapshot:
            with self.assertRaises(MinosSnapshotDeletedAggregateException):
                await snapshot.get("tests.aggregate_classes.Car", self.uuid_1)
            with self.assertRaises(MinosSnapshotAggregateNotFoundException):
                await snapshot.get("tests.aggregate_classes.Car", uuid4())

    async def test_get_with_transaction_raises(self):
        async with await self._populate() as snapshot:
            with self.assertRaises(MinosSnapshotDeletedAggregateException):
                await snapshot.get("tests.aggregate_classes.Car", self.uuid_2, transaction_uuid=self.second_transaction)

    async def test_find(self):
        async with await self._populate() as snapshot:
            condition = Condition.EQUAL("color", "blue")
            iterable = snapshot.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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
        async with await self._populate() as snapshot:
            iterable = snapshot.find("tests.aggregate_classes.Car", Condition.TRUE, Ordering.ASC("updated_at"))
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
                with self.assertRaises(MinosSnapshotDeletedAggregateException):
                    # noinspection PyStatementEffect
                    obs.build_aggregate()
            else:
                self.assertEqual(exp.build_aggregate(), obs.build_aggregate())
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)


if __name__ == "__main__":
    unittest.main()
