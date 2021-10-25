import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

import aiopg

from minos.common import (
    NULL_UUID,
    Action,
    FieldDiffContainer,
    MinosBrokerNotProvidedException,
    MinosRepository,
    PostgreSqlRepository,
    RepositoryEntry,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    assert_equal_repository_entries,
)


class TestPostgreSqlRepository(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid = uuid4()
        self.broker = FakeBroker()
        self.field_diff_container_patcher = patch(
            "minos.common.FieldDiffContainer.from_avro_bytes", return_value=FieldDiffContainer.empty()
        )
        self.field_diff_container_patcher.start()

    def tearDown(self):
        self.field_diff_container_patcher.stop()
        super().tearDown()

    def test_constructor(self):
        repository = PostgreSqlRepository("host", 1234, "database", "user", "password", event_broker=self.broker)
        self.assertIsInstance(repository, MinosRepository)
        self.assertEqual("host", repository.host)
        self.assertEqual(1234, repository.port)
        self.assertEqual("database", repository.database)
        self.assertEqual("user", repository.user)
        self.assertEqual("password", repository.password)

    async def test_constructor_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            PostgreSqlRepository("host", 1234, "database", "user", "password")

    async def test_setup(self):
        async with aiopg.connect(**self.repository_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                )
                response = (await cursor.fetchone())[0]
                self.assertFalse(response)

        async with PostgreSqlRepository(event_broker=self.broker, **self.repository_db):
            async with aiopg.connect(**self.repository_db) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                    )
                    response = (await cursor.fetchone())[0]
        self.assertTrue(response)

    async def test_generate_uuid(self):
        async with PostgreSqlRepository(event_broker=self.broker, **self.repository_db) as repository:
            await repository.create(RepositoryEntry(NULL_UUID, "example.Car", 1, bytes("foo", "utf-8")))
            observed = [v async for v in repository.select()]
            self.assertEqual(1, len(observed))
            self.assertIsInstance(observed[0].aggregate_uuid, UUID)
            self.assertNotEqual(NULL_UUID, observed[0].aggregate_uuid)

    async def test_create(self):
        async with PostgreSqlRepository(event_broker=self.broker, **self.repository_db) as repository:
            await repository.create(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))

            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE)]
            observed = [v async for v in repository.select()]
            assert_equal_repository_entries(self, expected, observed)

    async def test_update(self):
        async with PostgreSqlRepository(event_broker=self.broker, **self.repository_db) as repository:
            await repository.update(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.UPDATE)]
            observed = [v async for v in repository.select()]
            assert_equal_repository_entries(self, expected, observed)

    async def test_delete(self):
        async with PostgreSqlRepository(event_broker=self.broker, **self.repository_db) as repository:
            await repository.delete(RepositoryEntry(self.uuid, "example.Car", 1, bytes()))
            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.DELETE)]
            observed = [v async for v in repository.select()]
            assert_equal_repository_entries(self, expected, observed)

    async def test_select_empty(self):
        async with PostgreSqlRepository(event_broker=self.broker, **self.repository_db) as repository:
            expected = []
            observed = [v async for v in repository.select()]
            assert_equal_repository_entries(self, expected, observed)


class TestPostgreSqlRepositorySelect(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid = uuid4()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_4 = uuid4()

        self.first_transaction = uuid4()
        self.second_transaction = uuid4()

        self.broker = FakeBroker()
        self.field_diff_container_patcher = patch(
            "minos.common.FieldDiffContainer.from_avro_bytes", return_value=FieldDiffContainer.empty()
        )
        self.field_diff_container_patcher.start()

        self.entries = [
            RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            RepositoryEntry(
                self.uuid_2,
                "example.Car",
                3,
                bytes("hola", "utf-8"),
                8,
                Action.UPDATE,
                transaction_uuid=self.first_transaction,
            ),
            RepositoryEntry(
                self.uuid_2,
                "example.Car",
                3,
                bytes("salut", "utf-8"),
                9,
                Action.UPDATE,
                transaction_uuid=self.second_transaction,
            ),
            RepositoryEntry(
                self.uuid_2,
                "example.Car",
                4,
                bytes("adios", "utf-8"),
                10,
                Action.UPDATE,
                transaction_uuid=self.first_transaction,
            ),
        ]

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.repository = await self._build_repository()

    async def _build_repository(self):
        repository = PostgreSqlRepository(event_broker=self.broker, **self.repository_db)
        await repository.setup()
        await repository.create(RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8")))
        await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8")))
        await repository.create(RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8")))
        await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8")))
        await repository.delete(RepositoryEntry(self.uuid_1, "example.Car", 4))
        await repository.update(RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8")))
        await repository.create(RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8")))
        await repository.update(
            RepositoryEntry(
                self.uuid_2, "example.Car", 3, bytes("hola", "utf-8"), transaction_uuid=self.first_transaction
            )
        )
        await repository.update(
            RepositoryEntry(
                self.uuid_2, "example.Car", 3, bytes("salut", "utf-8"), transaction_uuid=self.second_transaction
            )
        )
        await repository.update(
            RepositoryEntry(
                self.uuid_2, "example.Car", 4, bytes("adios", "utf-8"), transaction_uuid=self.first_transaction
            )
        )
        return repository

    async def asyncTearDown(self):
        await self.repository.destroy()
        await super().asyncTearDown()

    def tearDown(self):
        self.field_diff_container_patcher.stop()
        super().tearDown()

    async def test_select(self):
        expected = self.entries
        observed = [v async for v in self.repository.select()]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id(self):
        expected = [
            self.entries[1],
        ]
        observed = [v async for v in self.repository.select(id=2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_lt(self):
        expected = self.entries[:4]

        observed = [v async for v in self.repository.select(id_lt=5)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_gt(self):
        expected = self.entries[4:]
        observed = [v async for v in self.repository.select(id_gt=4)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_le(self):
        expected = self.entries[:4]
        observed = [v async for v in self.repository.select(id_le=4)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_ge(self):
        expected = self.entries[4:]
        observed = [v async for v in self.repository.select(id_ge=5)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_aggregate_uuid(self):
        expected = [self.entries[2], self.entries[5], self.entries[7], self.entries[8], self.entries[9]]
        observed = [v async for v in self.repository.select(aggregate_uuid=self.uuid_2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_aggregate_name(self):
        expected = [self.entries[6]]
        observed = [v async for v in self.repository.select(aggregate_name="example.MotorCycle")]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version(self):
        expected = [self.entries[4], self.entries[9]]
        observed = [v async for v in self.repository.select(version=4)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_lt(self):
        expected = [self.entries[0], self.entries[2], self.entries[6]]
        observed = [v async for v in self.repository.select(version_lt=2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_gt(self):
        expected = [
            self.entries[1],
            self.entries[3],
            self.entries[4],
            self.entries[5],
            self.entries[7],
            self.entries[8],
            self.entries[9],
        ]
        observed = [v async for v in self.repository.select(version_gt=1)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_le(self):
        expected = [self.entries[0], self.entries[2], self.entries[6]]
        observed = [v async for v in self.repository.select(version_le=1)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_ge(self):
        expected = [
            self.entries[1],
            self.entries[3],
            self.entries[4],
            self.entries[5],
            self.entries[7],
            self.entries[8],
            self.entries[9],
        ]
        observed = [v async for v in self.repository.select(version_ge=2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_transaction_uuid_null(self):
        expected = self.entries[:7]
        observed = [v async for v in self.repository.select(transaction_uuid=NULL_UUID)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_transaction_uuid(self):
        expected = [self.entries[7], self.entries[9]]
        observed = [v async for v in self.repository.select(transaction_uuid=self.first_transaction)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_combined(self):
        expected = [self.entries[2], self.entries[5], self.entries[7], self.entries[8], self.entries[9]]
        observed = [v async for v in self.repository.select(aggregate_name="example.Car", aggregate_uuid=self.uuid_2)]
        assert_equal_repository_entries(self, expected, observed)


if __name__ == "__main__":
    unittest.main()
