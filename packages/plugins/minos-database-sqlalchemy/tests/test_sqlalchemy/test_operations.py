import unittest

from sqlalchemy.sql.elements import (
    TextClause,
)

from minos.common import (
    DatabaseOperation,
)
from minos.plugins.sqlalchemy import (
    SqlAlchemyDatabaseOperation,
)


class TestSqlAlchemyDatabaseOperation(unittest.TestCase):
    def test_subclass(self) -> None:
        self.assertTrue(issubclass(SqlAlchemyDatabaseOperation, DatabaseOperation))

    def test_constructor_str(self):
        operation = SqlAlchemyDatabaseOperation("query", {"foo": "bar"})
        self.assertIsInstance(operation.statement, TextClause)
        self.assertEqual("query", operation.statement.text)
        self.assertEqual({"foo": "bar"}, operation.parameters)
        self.assertEqual(None, operation.timeout)
        self.assertEqual(None, operation.lock)


if __name__ == "__main__":
    unittest.main()
