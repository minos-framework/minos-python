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

meta = MetaData()

foo = Table("foo", meta, Column("bar", String))


class TestSqlAlchemyDatabaseClient(unittest.IsolatedAsyncioTestCase):
    @unittest.skip
    async def test_execute(self):
        async with SqlAlchemyDatabaseClient("postgresql+asyncpg://postgres:postgres@localhost/postgres") as client:

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

            await client.recreate()

            operation = SqlAlchemyDatabaseOperation(meta.drop_all)
            await client.execute(operation)


if __name__ == "__main__":
    unittest.main()
