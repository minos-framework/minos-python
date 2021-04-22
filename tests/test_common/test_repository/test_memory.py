"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosInMemoryRepository,
    MinosRepository,
    MinosRepositoryAction,
    MinosRepositoryEntry,
)


class TestStringMethods(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        repository = MinosInMemoryRepository()
        self.assertIsInstance(repository, MinosRepository)

    async def test_insert(self):
        async with MinosInMemoryRepository() as repository:
            await repository.insert(MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))

            expected = [
                MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT)
            ]
            self.assertEqual(expected, await repository.select())

    async def test_update(self):
        async with MinosInMemoryRepository() as repository:
            await repository.update(MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [
                MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.UPDATE)
            ]
            self.assertEqual(expected, await repository.select())

    async def test_delete(self):
        async with MinosInMemoryRepository() as repository:
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

    async def test_select_empty(self):
        repository = MinosInMemoryRepository()
        self.assertEqual([], await repository.select())

    async def test_select_id(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(id=2))

    async def test_select_id_lt(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(id_lt=5))

    async def test_select_id_gt(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 4, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, await repository.select(id_gt=4))

    async def test_select_id_le(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(id_le=4))

    async def test_select_id_ge(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 4, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, await repository.select(id_ge=5))

    async def test_select_aggregate_id(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(aggregate_id=2))

    async def test_select_aggregate_name(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, await repository.select(aggregate_name="example.MotorCycle"))

    async def test_select_version(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 4, bytes(), 5, MinosRepositoryAction.DELETE),
        ]
        self.assertEqual(expected, await repository.select(version=4))

    async def test_select_version_lt(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, await repository.select(version_lt=2))

    async def test_select_version_gt(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 4, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(version_gt=1))

    async def test_select_version_le(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, await repository.select(version_le=1))

    async def test_select_version_ge(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 4, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(version_ge=2))

    async def test_select_combine(self):
        repository = await self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 4, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, await repository.select(aggregate_name="example.Car", id_ge=4))

    @staticmethod
    async def _build_repository():
        repository = MinosInMemoryRepository()
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
