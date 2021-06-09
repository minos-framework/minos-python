"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from datetime import (
    datetime,
)
from typing import (
    NoReturn,
)

import aiopg

from minos.common import (
    MinosRepository,
    PostgreSqlRepository,
    RepositoryAction,
    RepositoryEntry,
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
                template = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                await cursor.execute(template.format(**self.repository_db))
                response = (await cursor.fetchone())[0]
                self.assertFalse(response)

        repository = PostgreSqlRepository(**self.repository_db)
        await repository._setup()

        async with aiopg.connect(**self.repository_db) as connection:
            async with connection.cursor() as cursor:
                template = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
                await cursor.execute(template.format(**self.repository_db))
                response = (await cursor.fetchone())[0]
                self.assertTrue(response)

    async def test_aggregate(self):
        async with FakeBroker() as broker, PostgreSqlRepository(**self.repository_db) as repository:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository)
            await car.update(color="red")
            await car.update(doors=5)

            another = await Car.get_one(car.id, _broker=broker, _repository=repository)
            self.assertEqual(car, another)

            await car.delete()

    async def test_insert(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.insert(RepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))

            expected = [RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.INSERT)]
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_update(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.update(RepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.UPDATE)]
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_delete(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.delete(RepositoryEntry(0, "example.Car", 1, bytes()))
            expected = [RepositoryEntry(1, "example.Car", 1, bytes(), 1, RepositoryAction.DELETE)]
            observed = [v async for v in repository.select()]
            self._assert_equal_entries(expected, observed)

    async def test_select(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
                RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
                RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.INSERT),
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
                RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
            ]
            observed = [v async for v in repository.select(id=2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_lt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
                RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
            ]
            observed = [v async for v in repository.select(id_lt=5)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_gt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
                RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.INSERT),
            ]
            observed = [v async for v in repository.select(id_gt=4)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_le(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
                RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
            ]
            observed = [v async for v in repository.select(id_le=4)]
            self._assert_equal_entries(expected, observed)

    async def test_select_id_ge(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
                RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.INSERT),
            ]
            observed = [v async for v in repository.select(id_ge=5)]
            self._assert_equal_entries(expected, observed)

    async def test_select_aggregate_id(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.INSERT),
                RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
            ]
            observed = [v async for v in repository.select(aggregate_id=2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_aggregate_name(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.INSERT),
            ]
            observed = [v async for v in repository.select(aggregate_name="example.MotorCycle")]
            self._assert_equal_entries(expected, observed)

    async def test_select_version(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
            ]
            observed = [v async for v in repository.select(version=4)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_lt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.INSERT),
                RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.INSERT),
            ]
            observed = [v async for v in repository.select(version_lt=2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_gt(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
                RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
            ]
            observed = [v async for v in repository.select(version_gt=1)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_le(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.INSERT),
                RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.INSERT),
                RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.INSERT),
            ]
            observed = [v async for v in repository.select(version_le=1)]
            self._assert_equal_entries(expected, observed)

    async def test_select_version_ge(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
                RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
                RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
            ]
            observed = [v async for v in repository.select(version_ge=2)]
            self._assert_equal_entries(expected, observed)

    async def test_select_combined(self):
        async with (await self._build_repository()) as repository:
            expected = [
                RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.INSERT),
                RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
            ]
            observed = [v async for v in repository.select(aggregate_name="example.Car", aggregate_id=2)]
            self._assert_equal_entries(expected, observed)

    async def _build_repository(self):
        async with PostgreSqlRepository(**self.repository_db) as repository:
            await repository.insert(RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8")))
            await repository.update(RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8")))
            await repository.insert(RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8")))
            await repository.update(RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8")))
            await repository.delete(RepositoryEntry(1, "example.Car", 4))
            await repository.update(RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8")))
            await repository.insert(RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8")))
            return repository

    def _assert_equal_entries(self, expected: list[RepositoryEntry], observed: list[RepositoryEntry]) -> NoReturn:
        self.assertEqual(len(expected), len(observed))

        for e, o in zip(expected, observed):
            self.assertEqual(type(e), type(o))
            self.assertEqual(e.aggregate_id, o.aggregate_id)
            self.assertEqual(e.aggregate_name, o.aggregate_name)
            self.assertEqual(e.version, o.version)
            self.assertEqual(e.data, o.data)
            self.assertEqual(e.id, o.id)
            self.assertEqual(e.action, o.action)
            self.assertIsInstance(o.created_at, datetime)


if __name__ == "__main__":
    unittest.main()
