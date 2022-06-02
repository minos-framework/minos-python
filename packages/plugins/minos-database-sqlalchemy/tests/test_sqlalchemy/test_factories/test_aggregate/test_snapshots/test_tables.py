import unittest

from sqlalchemy import (
    MetaData,
    inspect,
)

from minos.aggregate import (
    Entity,
    Ref,
)
from minos.plugins.sqlalchemy import (
    SqlAlchemyDatabaseClient,
    SqlAlchemyDatabaseOperation,
    SqlAlchemySnapshotTableFactory,
)
from tests.utils import (
    SqlAlchemyTestCase,
)


class TestSqlAlchemySnapshotTableFactory(SqlAlchemyTestCase):
    def test_build(self):
        class _Foo(Entity):
            bar: str

        class _Bar(Entity):
            foo: Ref[_Foo]

        observed = SqlAlchemySnapshotTableFactory.build(_Foo, _Bar)

        self.assertIsInstance(observed, MetaData)

        self.assertEqual({_Foo.__name__, _Bar.__name__}, observed.tables.keys())

    async def test_build_create_tables(self):
        class _Foo(Entity):
            bar: str

        class _Bar(Entity):
            foo: Ref[_Foo]

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo, _Bar)

        create_operation = SqlAlchemyDatabaseOperation(metadata.create_all)
        drop_operation = SqlAlchemyDatabaseOperation(metadata.drop_all)

        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            observed = set(await client.connection.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names()))
            self.assertEqual(set(), observed)

            await client.execute(create_operation)

            observed = set(await client.connection.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names()))
            self.assertEqual({_Bar.__name__, _Foo.__name__}, observed)

            await client.execute(drop_operation)

            observed = set(await client.connection.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names()))
            self.assertEqual(set(), observed)


if __name__ == "__main__":
    unittest.main()
