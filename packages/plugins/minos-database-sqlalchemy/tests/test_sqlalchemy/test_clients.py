import unittest

from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    insert,
    select,
)

from minos.plugins.sqlalchemy import (
    SqlAlchemyDatabaseClient,
    SqlAlchemyDatabaseOperation,
)
from tests.utils import (
    SqlAlchemyTestCase,
)

meta = MetaData()

foo = Table("foo", meta, Column("bar", String))


class TestSqlAlchemyDatabaseClient(SqlAlchemyTestCase):
    async def test_execute(self):
        async with SqlAlchemyDatabaseClient(**self.config.get_default_database()) as client:
            operation = SqlAlchemyDatabaseOperation(meta.create_all)
            await client.execute(operation)

            operation = SqlAlchemyDatabaseOperation(
                insert(foo).values([{"bar": "one"}, {"bar": "two"}, {"bar": "three"}])
            )
            await client.execute(operation)

            operation = SqlAlchemyDatabaseOperation(select(foo))
            await client.execute(operation)

            async for row in client.fetch_all():
                print(row)

            await client.recreate()  # FIXME

            operation = SqlAlchemyDatabaseOperation(meta.drop_all)
            await client.execute(operation)


if __name__ == "__main__":
    unittest.main()
