import unittest

from minos.common import (
    AiopgDatabaseClient,
    AiopgDatabaseOperation,
    DatabaseClientPool,
    DatabaseMixin,
    NotProvidedException,
    PoolFactory,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from tests.utils import (
    CommonTestCase,
)


# noinspection SqlNoDataSourceInspection,SqlResolve
class TestDatabaseMixin(CommonTestCase, DatabaseMinosTestCase):
    def test_constructor(self):
        pool = DatabaseClientPool.from_config(self.config)
        database = DatabaseMixin(pool)
        self.assertEqual(pool, database.pool)

    async def test_constructor_with_pool_factory(self):
        pool_factory = PoolFactory(self.config, {"database": DatabaseClientPool})
        # noinspection PyTypeChecker
        database = DatabaseMixin(pool_factory=pool_factory)
        # noinspection PyUnresolvedReferences
        self.assertEqual(pool_factory.get_pool("database"), database.pool)

    async def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyArgumentEqualDefault
            DatabaseMixin(database_pool=None, pool_factory=None)

    async def test_pool(self):
        async with DatabaseMixin() as database:
            self.assertIsInstance(database.pool, DatabaseClientPool)

    async def test_submit_query(self):
        op1 = AiopgDatabaseOperation("CREATE TABLE foo (id INT NOT NULL);")
        op2 = AiopgDatabaseOperation("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'foo');")

        async with DatabaseMixin() as database:
            await database.submit_query(op1)

        async with AiopgDatabaseClient(**self.config.get_default_database()) as client:
            await client.execute(op2)
            self.assertTrue((await client.fetch_one())[0])

    async def test_submit_query_locked(self):
        op1 = AiopgDatabaseOperation("CREATE TABLE foo (id INT NOT NULL);", lock=1234)
        op2 = AiopgDatabaseOperation("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'foo');")

        async with DatabaseMixin() as database:
            await database.submit_query(op1)

        async with AiopgDatabaseClient(**self.config.get_default_database()) as client:
            await client.execute(op2)
            self.assertTrue((await client.fetch_one())[0])

    async def test_submit_query_and_fetchone(self):
        op1 = AiopgDatabaseOperation("CREATE TABLE foo (id INT NOT NULL);")
        op2 = AiopgDatabaseOperation("INSERT INTO foo (id) VALUES (3), (4), (5);")
        op3 = AiopgDatabaseOperation("SELECT * FROM foo;")

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)

            observed = await database.submit_query_and_fetchone(op3)

        self.assertEqual((3,), observed)

    async def test_submit_query_and_iter(self):
        op1 = AiopgDatabaseOperation("CREATE TABLE foo (id INT NOT NULL);")
        op2 = AiopgDatabaseOperation("INSERT INTO foo (id) VALUES (3), (4), (5);")
        op3 = AiopgDatabaseOperation("SELECT * FROM foo;")

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)
            observed = [v async for v in database.submit_query_and_iter(op3)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_streaming_mode_true(self):
        op1 = AiopgDatabaseOperation("CREATE TABLE foo (id INT NOT NULL);")
        op2 = AiopgDatabaseOperation("INSERT INTO foo (id) VALUES (3), (4), (5);")
        op3 = AiopgDatabaseOperation("SELECT * FROM foo;")

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)

            observed = [v async for v in database.submit_query_and_iter(op3, streaming_mode=True)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_locked(self):
        op1 = AiopgDatabaseOperation("CREATE TABLE foo (id INT NOT NULL);", lock=1234)
        op2 = AiopgDatabaseOperation("INSERT INTO foo (id) VALUES (3), (4), (5);")
        op3 = AiopgDatabaseOperation("SELECT * FROM foo;")

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)

            observed = [v async for v in database.submit_query_and_iter(op3)]

        self.assertEqual([(3,), (4,), (5,)], observed)


if __name__ == "__main__":
    unittest.main()
