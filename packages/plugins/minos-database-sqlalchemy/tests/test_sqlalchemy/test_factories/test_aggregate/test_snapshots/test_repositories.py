import unittest

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
    SqlAlchemySnapshotRepository,
)
from minos.transactions import (
    TransactionEntry,
)
from tests.utils import (
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


if __name__ == "__main__":
    unittest.main()
