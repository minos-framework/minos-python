import unittest
from datetime import (
    timedelta,
)
from uuid import (
    UUID,
    uuid4,
)

import aiopg

from minos.common import (
    NULL_UUID,
    Action,
    InMemorySnapshot,
    MinosRepository,
    PostgreSqlRepository,
    RepositoryEntry,
    current_datetime,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
)


class TestPostgreSqlRepository(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid = uuid4()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()

    def test_constructor(self):
        repository = PostgreSqlRepository("host", 1234, "database", "user", "password")
        self.assertIsInstance(repository, MinosRepository)
        self.assertEqual("host", repository.host)
        self.assertEqual(1234, repository.port)
        self.assertEqual("database", repository.database)
        self.assertEqual("user", repository.user)
        self.assertEqual("password", repository.password)

    async def test_setup(self):
        async with aiopg.connect(**self.repository_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                )
                response = (await cursor.fetchone())[0]
                self.assertFalse(response)

        async with PostgreSqlRepository(**self.repository_db):
            async with aiopg.connect(**self.repository_db) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                    )
                    response = (await cursor.fetchone())[0]
        self.assertTrue(response)

    async def test_aggregate(self):
        async with FakeBroker() as broker, PostgreSqlRepository(
            **self.repository_db
        ) as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            await car.update(color="red")
            await car.update(doors=5)

            another = await Car.get(car.uuid, _broker=broker, _repository=repository, _snapshot=snapshot)
            self.assertEqual(car, another)

            await car.delete()

    async def test_generate_uuid(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.create(RepositoryEntry(NULL_UUID, "example.Car", 1, bytes("foo", "utf-8")))
            observed = [v async for v in repository.select()]
            self.assertEqual(1, len(observed))
            self.assertIsInstance(observed[0].aggregate_uuid, UUID)
            self.assertNotEqual(NULL_UUID, observed[0].aggregate_uuid)

    async def test_create(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.create(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))

            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE)]
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_update(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.update(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.UPDATE)]
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_delete(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.delete(RepositoryEntry(self.uuid, "example.Car", 1, bytes()))
            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.DELETE)]
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_select(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
                RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
                RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            ]
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_select_empty(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            expected = []
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_select_id(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            ]
            observed = [v async for v in repository.select(id=2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_lt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
                RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            ]
            observed = [v async for v in repository.select(id_lt=5)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_gt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
                RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            ]
            observed = [v async for v in repository.select(id_gt=4)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_le(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
                RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            ]
            observed = [v async for v in repository.select(id_le=4)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_ge(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
                RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            ]
            observed = [v async for v in repository.select(id_ge=5)]
            self._assert_equal_entries(expected, observed)

    async def test_select_aggregate_uuid(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
                RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            ]
            observed = [v async for v in repository.select(aggregate_uuid=self.uuid_2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_aggregate_name(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            ]
            observed = [v async for v in repository.select(aggregate_name="example.MotorCycle")]
            self._assert_equal_entries(expected, observed)

    async def test_select_version(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            ]
            observed = [v async for v in repository.select(version=4)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_lt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
                RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            ]
            observed = [v async for v in repository.select(version_lt=2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_gt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
                RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            ]
            observed = [v async for v in repository.select(version_gt=1)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_le(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
                RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
                RepositoryEntry(self.uuid_1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            ]
            observed = [v async for v in repository.select(version_le=1)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_ge(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
                RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
                RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            ]
            observed = [v async for v in repository.select(version_ge=2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_combined(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
                RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            ]
            observed = [v async for v in repository.select(aggregate_name="example.Car", aggregate_uuid=self.uuid_2)]
            self._assert_equal_entries(expected, observed)

    async def _build_repository(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.create(RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8")))
            await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8")))
            await repository.create(RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8")))
            await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8")))
            await repository.delete(RepositoryEntry(self.uuid_1, "example.Car", 4))
            await repository.update(RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8")))
            await repository.create(RepositoryEntry(self.uuid_1, "example.MotorCycle", 1, bytes("one", "utf-8")))
            return repository

    def _assert_equal_entries(self, expected: list[RepositoryEntry], observed: list[RepositoryEntry]) -> None:
        self.assertEqual(len(expected), len(observed))

        for e, o in zip(expected, observed):
            self.assertEqual(type(e), type(o))
            self.assertEqual(e.aggregate_uuid, o.aggregate_uuid)
            self.assertEqual(e.aggregate_name, o.aggregate_name)
            self.assertEqual(e.version, o.version)
            self.assertEqual(e.data, o.data)
            self.assertEqual(e.id, o.id)
            self.assertEqual(e.action, o.action)
            self.assertAlmostEqual(current_datetime(), o.created_at, delta=timedelta(seconds=5))


if __name__ == "__main__":
    unittest.main()
