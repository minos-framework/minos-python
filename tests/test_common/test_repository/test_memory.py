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

        expected = [MinosRepositoryEntry(0, "example.Car", 0, bytes("foo", "utf-8"), 0, MinosRepositoryAction.INSERT)]
        self.assertEqual(expected, repository.select())

    def test_update(self):
        repository = MinosInMemoryRepository()
        repository.update(MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8")))
        expected = [MinosRepositoryEntry(0, "example.Car", 1, bytes("foo", "utf-8"), 0, MinosRepositoryAction.UPDATE)]
        self.assertEqual(expected, repository.select())

    def test_delete(self):
        repository = MinosInMemoryRepository()
        repository.delete(MinosRepositoryEntry(0, "example.Car", 1, bytes()))
        expected = [MinosRepositoryEntry(0, "example.Car", 1, bytes(), 0, MinosRepositoryAction.DELETE)]
        self.assertEqual(expected, repository.select())

    def test_combined(self):
        repository = MinosInMemoryRepository()
        repository.insert(MinosRepositoryEntry(0, "example.Car", 0, bytes("foo", "utf-8")))
        repository.update(MinosRepositoryEntry(0, "example.Car", 1, bytes("bar", "utf-8")))
        repository.insert(MinosRepositoryEntry(1, "example.Car", 0, bytes("hello", "utf-8")))
        repository.update(MinosRepositoryEntry(0, "example.Car", 2, bytes("foobar", "utf-8")))
        repository.delete(MinosRepositoryEntry(0, "example.Car", 3))
        repository.update(MinosRepositoryEntry(1, "example.Car", 1, bytes("bye", "utf-8")))
        expected = [
            MinosRepositoryEntry(0, "example.Car", 0, bytes("foo", "utf-8"), 0, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(0, "example.Car", 1, bytes("bar", "utf-8"), 1, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(1, "example.Car", 0, bytes("hello", "utf-8"), 2, MinosRepositoryAction.INSERT),
            MinosRepositoryEntry(0, "example.Car", 2, bytes("foobar", "utf-8"), 3, MinosRepositoryAction.UPDATE),
            MinosRepositoryEntry(0, "example.Car", 3, bytes(), 4, MinosRepositoryAction.DELETE),
            MinosRepositoryEntry(1, "example.Car", 1, bytes("bye", "utf-8"), 5, MinosRepositoryAction.UPDATE),
        ]
        self.assertEqual(expected, repository.select())


if __name__ == "__main__":
    unittest.main()
