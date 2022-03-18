import unittest
from datetime import (
    datetime,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    AlreadyDeletedException,
    Condition,
    EventEntry,
    FieldDiff,
    FieldDiffContainer,
    NotFoundException,
    Ordering,
    PostgreSqlSnapshotReader,
    PostgreSqlSnapshotSetup,
    PostgreSqlSnapshotWriter,
    SnapshotEntry,
    TransactionEntry,
    TransactionStatus,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
    Car,
    MinosTestCase,
)


class TestPostgreSqlSnapshotReader(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()

        self.transaction_1 = uuid4()
        self.transaction_2 = uuid4()
        self.transaction_3 = uuid4()

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
        async with PostgreSqlSnapshotWriter.from_config(self.config, reader=self.reader) as writer:
            await writer.dispatch()

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

        iterable = self.reader.find("tests.utils.Car", condition, ordering=Ordering.ASC("updated_at"))
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
            "tests.utils.Car",
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

        iterable = self.reader.find(
            "tests.utils.Car",
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

        iterable = self.reader.find(
            "tests.utils.Car", condition, ordering=Ordering.ASC("updated_at"), transaction_uuid=self.transaction_3
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

        iterable = self.reader.find(
            "tests.utils.Car", condition, streaming_mode=True, ordering=Ordering.ASC("updated_at")
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

        iterable = self.reader.find("tests.utils.Car", condition, ordering=Ordering.ASC("updated_at"))
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

        observed = {v async for v in self.reader.find("tests.utils.Car", Condition.FALSE)}

        expected = set()
        self.assertEqual(expected, observed)

    async def test_get(self):

        observed = await self.reader.get("tests.utils.Car", self.uuid_2)

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=2, created_at=observed.created_at, updated_at=observed.updated_at
        )
        self.assertEqual(expected, observed)

    async def test_get_with_transaction(self):

        observed = await self.reader.get(
            "tests.utils.Car", self.uuid_2, transaction=TransactionEntry(self.transaction_1)
        )

        expected = Car(
            3, "blue", uuid=self.uuid_2, version=4, created_at=observed.created_at, updated_at=observed.updated_at
        )
        self.assertEqual(expected, observed)

    async def test_get_raises(self):

        with self.assertRaises(AlreadyDeletedException):
            await self.reader.get("tests.utils.Car", self.uuid_1)
        with self.assertRaises(NotFoundException):
            await self.reader.get("tests.utils.Car", uuid4())

    async def test_get_with_transaction_raises(self):

        with self.assertRaises(AlreadyDeletedException):
            await self.reader.get("tests.utils.Car", self.uuid_2, transaction=TransactionEntry(self.transaction_2))

    async def test_find(self):
        condition = Condition.EQUAL("color", "blue")
        iterable = self.reader.find("tests.utils.Car", condition, ordering=Ordering.ASC("updated_at"))
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

        iterable = self.reader.find("tests.utils.Car", Condition.TRUE, Ordering.ASC("updated_at"))
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


if __name__ == "__main__":
    unittest.main()
