import unittest
from abc import (
    ABC,
)

from minos.common import (
    ComposedDatabaseOperation,
    DatabaseOperation,
    DatabaseOperationFactory,
)


class _DatabaseOperation(DatabaseOperation):
    """For testing purposes."""


class TestDatabaseOperation(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(DatabaseOperation, ABC))

    def test_constructor(self):
        operation = _DatabaseOperation()
        self.assertEqual(None, operation.timeout)
        self.assertEqual(None, operation.lock)

    def test_constructor_extended(self):
        operation = _DatabaseOperation(timeout=3, lock="foo")
        self.assertEqual(3, operation.timeout)
        self.assertEqual("foo", operation.lock)


class TestComposedDatabaseOperation(unittest.TestCase):
    def test_subclass(self) -> None:
        self.assertTrue(issubclass(ComposedDatabaseOperation, DatabaseOperation))

    def test_constructor(self):
        operations = [_DatabaseOperation(), _DatabaseOperation()]
        composed = ComposedDatabaseOperation(operations)
        self.assertEqual(tuple(operations), composed.operations)


class TestDatabaseOperationFactory(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(DatabaseOperationFactory, ABC))


if __name__ == "__main__":
    unittest.main()
