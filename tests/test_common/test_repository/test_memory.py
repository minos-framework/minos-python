"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    InMemoryRepository,
    MinosRepository,
    RepositoryAction,
    RepositoryEntry,
)


class TestStringMethods(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        repository = InMemoryRepository()
        self.assertIsInstance(repository, MinosRepository)

    async def test_create(self):
        async with InMemoryRepository() as repository:
            await repository.create(RepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))

            expected = [RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.CREATE)]
            self.assertEqual(expected, [v async for v in repository.select()])

    async def test_update(self):
        async with InMemoryRepository() as repository:
            await repository.update(RepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.UPDATE)]
            self.assertEqual(expected, [v async for v in repository.select()])

    async def test_delete(self):
        async with InMemoryRepository() as repository:
            await repository.delete(RepositoryEntry(0, "example.Car", 1, bytes()))
            expected = [RepositoryEntry(1, "example.Car", 1, bytes(), 1, RepositoryAction.DELETE)]
            self.assertEqual(expected, [v async for v in repository.select()])

    async def test_select(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
            RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
            RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.CREATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select()])

    async def test_select_empty(self):
        repository = InMemoryRepository()
        self.assertEqual([], [v async for v in repository.select()])

    async def test_select_id(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(id=2)])

    async def test_select_id_lt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
            RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(id_lt=5)])

    async def test_select_id_gt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
            RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.CREATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(id_gt=4)])

    async def test_select_id_le(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
            RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(id_le=4)])

    async def test_select_id_ge(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
            RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.CREATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(id_ge=5)])

    async def test_select_aggregate_id(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.CREATE),
            RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(aggregate_id=2)])

    async def test_select_aggregate_name(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.CREATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(aggregate_name="example.MotorCycle")])

    async def test_select_version(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(version=4)])

    async def test_select_version_lt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.CREATE),
            RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.CREATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(version_lt=2)])

    async def test_select_version_gt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
            RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(version_gt=1)])

    async def test_select_version_le(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8"), 1, RepositoryAction.CREATE),
            RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8"), 3, RepositoryAction.CREATE),
            RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, RepositoryAction.CREATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(version_le=1)])

    async def test_select_version_ge(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8"), 2, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
            RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(version_ge=2)])

    async def test_select_combine(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8"), 4, RepositoryAction.UPDATE),
            RepositoryEntry(1, "example.Car", 4, bytes(), 5, RepositoryAction.DELETE),
            RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8"), 6, RepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, [v async for v in repository.select(aggregate_name="example.Car", id_ge=4)])

    @staticmethod
    async def _build_repository():
        repository = InMemoryRepository()
        await repository.create(RepositoryEntry(1, "example.Car", 1, bytes("foo", "utf-8")))
        await repository.update(RepositoryEntry(1, "example.Car", 2, bytes("bar", "utf-8")))
        await repository.create(RepositoryEntry(2, "example.Car", 1, bytes("hello", "utf-8")))
        await repository.update(RepositoryEntry(1, "example.Car", 3, bytes("foobar", "utf-8")))
        await repository.delete(RepositoryEntry(1, "example.Car", 4))
        await repository.update(RepositoryEntry(2, "example.Car", 2, bytes("bye", "utf-8")))
        await repository.create(RepositoryEntry(1, "example.MotorCycle", 1, bytes("one", "utf-8")))

        return repository


if __name__ == "__main__":
    unittest.main()
