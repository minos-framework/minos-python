"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

import aiopg
from minos.common import (
    MinosInMemoryRepository,
    MinosRepository,
    MinosRepositoryAction,
    MinosRepositoryEntry,
    PostgreSqlMinosRepository,
)
from tests.aggregate_classes import (
    Car,
)
from tests.database_testcase import (
    PostgresAsyncTestCase,
)


class TestPostgreSqlMinosRepository(PostgresAsyncTestCase):
    def test_constructor(self):
        repository = PostgreSqlMinosRepository(**self.kwargs)
        self.assertIsInstance(repository, MinosRepository)
        self.assertEqual(self.kwargs["host"], repository.host)
        self.assertEqual(self.kwargs["port"], repository.port)
        self.assertEqual(self.kwargs["database"], repository.database)
        self.assertEqual(self.kwargs["user"], repository.user)
        self.assertEqual(self.kwargs["password"], repository.password)

    async def test_setup(self):
        async with aiopg.connect(**self.kwargs) as connection:
            async with connection.cursor() as cursor:
                template = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'events');"
                await cursor.execute(template.format(**self.kwargs))
                response = (await cursor.fetchone())[0]
                self.assertFalse(response)

        repository = PostgreSqlMinosRepository(**self.kwargs)
        await repository.setup()

        async with aiopg.connect(**self.kwargs) as connection:
            async with connection.cursor() as cursor:
                template = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'events');"
                await cursor.execute(template.format(**self.kwargs))
                response = (await cursor.fetchone())[0]
                self.assertTrue(response)

    async def test_aggregate(self):
        async with PostgreSqlMinosRepository(**self.kwargs) as repository:
            car = await Car.create(doors=3, color="blue", _repository=repository)
            await car.update(color="red")
            await car.update(doors=5)

            another = await Car.get_one(car.id, _repository=repository)
            self.assertEqual(car, another)

            await car.delete()

    async def test_insert(self):
        async with PostgreSqlMinosRepository(**self.kwargs) as repository:
            await repository.insert(MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))

            expected = [
                MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT)
            ]
            self.assertEqual(expected, await repository.select())

    async def test_update(self):
        async with PostgreSqlMinosRepository(**self.kwargs) as repository:
            await repository.update(MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [
                MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.UPDATE)
            ]
            self.assertEqual(expected, await repository.select())

    async def test_delete(self):
        async with PostgreSqlMinosRepository(**self.kwargs) as repository:
            repository = MinosInMemoryRepository()
            await repository.delete(MinosRepositoryEntry(0, "example.Car", 1, bytes()))
            expected = [MinosRepositoryEntry(0, "example.Car", 1, bytes(), 1, MinosRepositoryAction.DELETE)]
            self.assertEqual(expected, await repository.select())

    async def test_select(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 4, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, await repository.select())

    async def test_select_filtered(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(aggregate_name="example.Car", aggregate_id=2))

    async def _build_repository(self):
        repository = PostgreSqlMinosRepository(**self.kwargs)
        await repository.setup()
        await repository.insert(MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8")))
        await repository.update(MinosRepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8")))
        await repository.insert(MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8")))
        await repository.update(MinosRepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8")))
        await repository.delete(MinosRepositoryEntry(1, "example.Car", 4))
        await repository.update(MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8")))
        await repository.insert(MinosRepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8")))

        return repository


if __name__ == "__main__":
    unittest.main()
