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
    MinosSnapshotAggregateNotFoundException,
    MinosSnapshotDeletedAggregateException,
    Ordering,
    PostgreSqlSnapshotReader,
    PostgreSqlSnapshotSetup,
    PostgreSqlSnapshotWriter,
    RepositoryEntry,
    SnapshotEntry,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
    MinosTestCase,
)


class TestPostgreSqlSnapshotReader(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()

        self.first_transaction = uuid4()
        self.second_transaction = uuid4()

        self.reader = PostgreSqlSnapshotReader.from_config(self.config)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.reader.setup()
        await self._populate()

    async def asyncTearDown(self):
        await self.reader.destroy()
        await super().asyncTearDown()

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
            RepositoryEntry(self.uuid_2, aggregate_name, 3, diff.avro_bytes, transaction_uuid=self.first_transaction)
        )
        await self.repository.delete(
            RepositoryEntry(self.uuid_2, aggregate_name, 3, bytes(), transaction_uuid=self.second_transaction)
        )
        await self.repository.update(
            RepositoryEntry(self.uuid_2, aggregate_name, 4, diff.avro_bytes, transaction_uuid=self.first_transaction)
        )
        await self.repository.create(RepositoryEntry(self.uuid_3, aggregate_name, 1, diff.avro_bytes))

        async with PostgreSqlSnapshotWriter.from_config(
            self.config, repository=self.repository, transaction_repository=self.transaction_repository
        ) as dispatcher:
            await dispatcher.dispatch()

    def test_type(self):
        self.assertTrue(issubclass(PostgreSqlSnapshotReader, PostgreSqlSnapshotSetup))

    def test_from_config(self):
        reader = PostgreSqlSnapshotReader.from_config(self.config)
        self.assertEqual(self.config.snapshot.host, reader.host)
        self.assertEqual(self.config.snapshot.port, reader.port)
        self.assertEqual(self.config.snapshot.database, reader.database)
        self.assertEqual(self.config.snapshot.user, reader.user)
        self.assertEqual(self.config.snapshot.password, reader.password)

    async def test_find_by_uuid(self):
        condition = Condition.IN("uuid", [self.uuid_2, self.uuid_3])

        iterable = self.reader.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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

        iterable = self.reader.find(
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

        iterable = self.reader.find(
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

        iterable = self.reader.find(
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

        iterable = self.reader.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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

        observed = {v async for v in self.reader.find("tests.aggregate_classes.Car", Condition.FALSE)}

        expected = set()
        self.assertEqual(expected, observed)

    async def test_get(self):

        observed = await self.reader.get("tests.aggregate_classes.Car", self.uuid_2)

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=2, created_at=observed.created_at, updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_get_with_transaction(self):

        observed = await self.reader.get(
            "tests.aggregate_classes.Car", self.uuid_2, transaction_uuid=self.first_transaction
        )

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=4, created_at=observed.created_at, updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_get_raises(self):

        with self.assertRaises(MinosSnapshotDeletedAggregateException):
            await self.reader.get("tests.aggregate_classes.Car", self.uuid_1)
        with self.assertRaises(MinosSnapshotAggregateNotFoundException):
            await self.reader.get("tests.aggregate_classes.Car", uuid4())

    async def test_get_with_transaction_raises(self):

        with self.assertRaises(MinosSnapshotDeletedAggregateException):
            await self.reader.get("tests.aggregate_classes.Car", self.uuid_2, transaction_uuid=self.second_transaction)

    async def test_find(self):

        condition = Condition.EQUAL("color", "blue")
        iterable = self.reader.find("tests.aggregate_classes.Car", condition, ordering=Ordering.ASC("updated_at"))
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

        iterable = self.reader.find("tests.aggregate_classes.Car", Condition.TRUE, Ordering.ASC("updated_at"))
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
