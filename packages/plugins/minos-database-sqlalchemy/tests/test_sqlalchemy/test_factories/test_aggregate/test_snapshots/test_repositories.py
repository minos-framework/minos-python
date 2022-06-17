import unittest
from unittest.mock import (
    MagicMock,
)

from sqlalchemy import (
    select,
)
from sqlalchemy.sql import (
    Subquery,
)

from minos.aggregate.testing import (
    SnapshotRepositoryTestCase,
)
from minos.common import (
    ModelType,
    ProgrammingException,
    classname,
)
from minos.plugins.sqlalchemy import (
    SqlAlchemyDatabaseOperation,
    SqlAlchemySnapshotRepository,
)
from minos.transactions import (
    TransactionEntry,
)
from tests.utils import (
    FakeAsyncIterator,
    SqlAlchemyTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestSqlAlchemySnapshotRepository(SqlAlchemyTestCase, SnapshotRepositoryTestCase):
    __test__ = True

    def build_snapshot_repository(self):
        return SqlAlchemySnapshotRepository.from_config(self.config)

    async def test_find_contains(self):
        # The contains condition does not apply to standard SQL databases.
        with self.assertRaises(ProgrammingException):
            await super().test_find_contains()

    async def test_get_table(self):
        table = await self.snapshot_repository.get_table(self.Car)

        self.assertIsInstance(table, Subquery)

    async def test_get_table_from_classname(self):
        table = await self.snapshot_repository.get_table(classname(self.Car))

        self.assertIsInstance(table, Subquery)

    async def test_get_table_from_model_type(self):
        table = await self.snapshot_repository.get_table(ModelType.from_model(self.Car))

        self.assertIsInstance(table, Subquery)

    async def test_get_table_with_transaction(self):
        async with TransactionEntry():
            table = await self.snapshot_repository.get_table(self.Car)

            self.assertIsInstance(table, Subquery)

    async def test_execute_statement(self):
        table = await self.snapshot_repository.get_table(self.Car)

        statement = select(table)
        expected = [{"number": "one"}, {"number": "two"}, {"number": "three"}]

        mock = MagicMock(return_value=FakeAsyncIterator(expected))
        self.snapshot_repository.execute_on_database_and_fetch_all = mock

        observed = [row async for row in self.snapshot_repository.execute_statement(statement)]

        self.assertEqual(expected, observed)

        self.assertIsInstance(mock.call_args.args[0], SqlAlchemyDatabaseOperation)
        self.assertEqual(statement, mock.call_args.args[0].statement)


if __name__ == "__main__":
    unittest.main()
