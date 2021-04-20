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


class TestStringMethods(unittest.TestCase):
    def test_constructor(self):
        repository = MinosInMemoryRepository()
        self.assertIsInstance(repository, MinosRepository)

    def test_insert(self):
        repository = MinosInMemoryRepository()
        repository.insert(MinosRepositoryEntry(0, "example.Car", 0, bytes("foo", "utf-8")))

        expected = [MinosRepositoryEntry(0, "example.Car", 0, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT)]
        self.assertEqual(expected, repository.select())

    def test_update(self):
        repository = MinosInMemoryRepository()
        repository.update(MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))
        expected = [MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8"), 1, MinosRepositoryAction.UPDATE)]
        self.assertEqual(expected, repository.select())

    def test_delete(self):
        repository = MinosInMemoryRepository()
        repository.delete(MinosRepositoryEntry(0, "example.Car", 1, bytes()))
        expected = [MinosRepositoryEntry(0, "example.Car", 1, bytes(), 1, MinosRepositoryAction.DELETE)]
        self.assertEqual(expected, repository.select())

    def test_select(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 0, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 1, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(2, "example.Car", 0, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 3, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.MotorCycle", 0, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, repository.select())

    def test_select_id(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select(id=2))

    def test_select_id_lt(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 0, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 1, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(2, "example.Car", 0, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select(id_lt=5))

    def test_select_id_gt(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 3, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.MotorCycle", 0, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, repository.select(id_gt=4))

    def test_select_id_le(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 0, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 1, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(2, "example.Car", 0, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select(id_le=4))

    def test_select_id_ge(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 3, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.MotorCycle", 0, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, repository.select(id_ge=5))

    def test_select_aggregate_id(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(2, "example.Car", 0, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select(aggregate_id=2))

    def test_select_aggregate_name(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.MotorCycle", 0, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, repository.select(aggregate_name="example.MotorCycle"))

    def test_select_version(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 3, bytes(), 5, MinosRepositoryAction.DELETE),
        ]
        self.assertEqual(expected, repository.select(version=3))

    def test_select_version_lt(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 0, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(2, "example.Car", 0, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.MotorCycle", 0, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, repository.select(version_lt=1))

    def test_select_version_gt(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 3, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select(version_gt=0))

    def test_select_version_le(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 0, bytes("foo", "utf-8"), 1, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(2, "example.Car", 0, bytes("hello", "utf-8"), 3, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(1, "example.MotorCycle", 0, bytes("one", "utf-8"), 7, MinosRepositoryAction.INSERT),
        ]
        self.assertEqual(expected, repository.select(version_le=0))

    def test_select_version_ge(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 1, bytes("bar", "utf-8"), 2, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 2, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 3, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select(version_ge=1))

    def test_select_combine(self):
        repository = self._build_repository()
        expected = [
            MinosRepositoryEntry(1, "example.Car", 2, bytes("foobar", "utf-8"), 4, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 3, bytes(), 5, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8"), 6, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select(aggregate_name="example.Car", id_ge=4))

    @staticmethod
    def _build_repository():
        repository = MinosInMemoryRepository()
        repository.insert(MinosRepositoryEntry(1, "example.Car", 0, bytes("foo", "utf-8")))
        repository.update(MinosRepositoryEntry(1, "example.Car", 1, bytes("bar", "utf-8")))
        repository.insert(MinosRepositoryEntry(2, "example.Car", 0, bytes("hello", "utf-8")))
        repository.update(MinosRepositoryEntry(1, "example.Car", 2, bytes("foobar", "utf-8")))
        repository.delete(MinosRepositoryEntry(1, "example.Car", 3))
        repository.update(MinosRepositoryEntry(2, "example.Car", 1, bytes("bye", "utf-8")))
        repository.insert(MinosRepositoryEntry(1, "example.MotorCycle", 0, bytes("one", "utf-8")))

        return repository


if __name__ == "__main__":
    unittest.main()
