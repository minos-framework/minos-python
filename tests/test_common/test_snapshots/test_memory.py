import unittest
from datetime import (
    datetime,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Condition,
    FieldDiff,
    FieldDiffContainer,
    InMemorySnapshot,
    MinosRepositoryNotProvidedException,
    MinosSnapshot,
    MinosSnapshotAggregateNotFoundException,
    MinosSnapshotDeletedAggregateException,
    MinosTransactionRepositoryNotProvidedException,
    Ordering,
    RepositoryEntry,
    SnapshotEntry,
    Transaction,
    TransactionStatus,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    MinosTestCase,
)


class TestInMemorySnapshot(MinosTestCase):
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
        aggregate_name: str = Car.classname
        await self.repository.create(RepositoryEntry(self.uuid_1, aggregate_name, 1, diff.avro_bytes))
        await self.repository.update(RepositoryEntry(self.uuid_1, aggregate_name, 2, diff.avro_bytes))
        await self.repository.create(RepositoryEntry(self.uuid_2, aggregate_name, 1, diff.avro_bytes))
        await self.repository.update(RepositoryEntry(self.uuid_1, aggregate_name, 3, diff.avro_bytes))
        await self.repository.delete(RepositoryEntry(self.uuid_1, aggregate_name, 4))
        await self.repository.update(RepositoryEntry(self.uuid_2, aggregate_name, 2, diff.avro_bytes))
        await self.repository.update(
            RepositoryEntry(self.uuid_2, aggregate_name, 3, diff.avro_bytes, transaction_uuid=self.transaction_1)
        )
        await self.repository.delete(
            RepositoryEntry(self.uuid_2, aggregate_name, 3, bytes(), transaction_uuid=self.transaction_2)
        )
        await self.repository.update(
            RepositoryEntry(self.uuid_2, aggregate_name, 4, diff.avro_bytes, transaction_uuid=self.transaction_1)
        )
        await self.repository.create(RepositoryEntry(self.uuid_3, aggregate_name, 1, diff.avro_bytes))
        await self.repository.delete(
            RepositoryEntry(self.uuid_2, aggregate_name, 3, bytes(), transaction_uuid=self.transaction_3)
        )
        await self.transaction_repository.submit(
            Transaction(self.transaction_1, TransactionStatus.PENDING, await self.repository.offset)
        )
        await self.transaction_repository.submit(
            Transaction(self.transaction_2, TransactionStatus.PENDING, await self.repository.offset)
        )
        await self.transaction_repository.submit(
            Transaction(self.transaction_3, TransactionStatus.REJECTED, await self.repository.offset)
        )

    def test_type(self):
        self.assertTrue(issubclass(InMemorySnapshot, MinosSnapshot))

    def test_constructor_raises(self):
        with self.assertRaises(MinosRepositoryNotProvidedException):
            # noinspection PyTypeChecker
            InMemorySnapshot(repository=None)

        with self.assertRaises(MinosTransactionRepositoryNotProvidedException):
            # noinspection PyTypeChecker
            InMemorySnapshot(transaction_repository=None)

    async def test_find_by_uuid(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])
        iterable = self.snapshot.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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
        iterable = self.snapshot.find(
            "tests.aggregate_classes.Car",
            condition,
            ordering=Ordering.ASC("updated_at"),
            transaction_uuid=self.transaction_1,
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
        iterable = self.snapshot.find(
            "tests.aggregate_classes.Car",
            condition,
            ordering=Ordering.ASC("updated_at"),
            transaction_uuid=self.transaction_2,
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
        iterable = self.snapshot.find(
            "tests.aggregate_classes.Car",
            condition,
            ordering=Ordering.ASC("updated_at"),
            transaction_uuid=self.transaction_3,
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

        iterable = self.snapshot.find(
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
        iterable = self.snapshot.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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
        observed = {v async for v in self.snapshot.find("tests.aggregate_classes.Car", Condition.FALSE)}

        expected = set()
        self.assertEqual(expected, observed)

    async def test_get(self):
        observed = await self.snapshot.get("tests.aggregate_classes.Car", self.uuid_2)

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=2, created_at=observed.created_at, updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_get_with_transaction(self):
        observed = await self.snapshot.get(
            "tests.aggregate_classes.Car", self.uuid_2, transaction_uuid=self.transaction_1
        )

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=4, created_at=observed.created_at, updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_get_raises(self):
        with self.assertRaises(MinosSnapshotDeletedAggregateException):
            await self.snapshot.get("tests.aggregate_classes.Car", self.uuid_1)
        with self.assertRaises(MinosSnapshotAggregateNotFoundException):
            await self.snapshot.get("tests.aggregate_classes.Car", uuid4())

    async def test_get_with_transaction_raises(self):
        with self.assertRaises(MinosSnapshotDeletedAggregateException):
            await self.snapshot.get("tests.aggregate_classes.Car", self.uuid_2, transaction_uuid=self.transaction_2)

    async def test_find(self):
        condition = Condition.EQUAL("color", "blue")
        iterable = self.snapshot.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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
        iterable = self.snapshot.find("tests.aggregate_classes.Car", Condition.TRUE, Ordering.ASC("updated_at"))
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
