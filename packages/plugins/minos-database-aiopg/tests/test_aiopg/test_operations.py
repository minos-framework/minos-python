import unittest

from minos.common import (
    DatabaseOperation,
)
from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
)


class TestAiopgDatabaseOperation(unittest.TestCase):
    def test_subclass(self) -> None:
        self.assertTrue(issubclass(AiopgDatabaseOperation, DatabaseOperation))

    def test_constructor(self):
        operation = AiopgDatabaseOperation("query", {"foo": "bar"})
        self.assertEqual("query", operation.query)
        self.assertEqual({"foo": "bar"}, operation.parameters)
        self.assertEqual(None, operation.timeout)
        self.assertEqual(None, operation.lock)


if __name__ == "__main__":
    unittest.main()
