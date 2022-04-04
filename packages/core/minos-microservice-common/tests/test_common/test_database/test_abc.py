import unittest
import warnings

import aiopg

from minos.common import (
    DatabaseClientPool,
    DatabaseMixin,
    NotProvidedException,
    PoolFactory,
    PostgreSqlMinosDatabase,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CommonTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseMixin(CommonTestCase, PostgresAsyncTestCase):
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
            DatabaseMixin(pool=None, pool_factory=None)

    def test_database(self):
        pool = DatabaseClientPool.from_config(self.config)
        database = DatabaseMixin(pool)
        self.assertEqual(pool.database, database.database)

    def test_user(self):
        pool = DatabaseClientPool.from_config(self.config)
        database = DatabaseMixin(pool)
        self.assertEqual(pool.user, database.user)

    def test_password(self):
        pool = DatabaseClientPool.from_config(self.config)
        database = DatabaseMixin(pool)
        self.assertEqual(pool.password, database.password)

    def test_host(self):
        pool = DatabaseClientPool.from_config(self.config)
        database = DatabaseMixin(pool)
        self.assertEqual(pool.host, database.host)

    def test_port(self):
        pool = DatabaseClientPool.from_config(self.config)
        database = DatabaseMixin(pool)
        self.assertEqual(pool.port, database.port)

    async def test_pool(self):
        async with DatabaseMixin() as database:
            self.assertIsInstance(database.pool, DatabaseClientPool)

    async def test_submit_query(self):
        async with DatabaseMixin() as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")

        async with aiopg.connect(**self.config.get_default_database()) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'foo');")
                self.assertTrue((await cursor.fetchone())[0])

    async def test_submit_query_locked(self):
        async with DatabaseMixin() as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);", lock=1234)

        async with aiopg.connect(**self.config.get_default_database()) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'foo');")
                self.assertTrue((await cursor.fetchone())[0])

    async def test_submit_query_and_fetchone(self):
        async with DatabaseMixin() as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = await database.submit_query_and_fetchone("SELECT * FROM foo;")

        self.assertEqual((3,), observed)

    async def test_submit_query_and_iter(self):
        async with DatabaseMixin() as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = [v async for v in database.submit_query_and_iter("SELECT * FROM foo;")]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_streaming_mode_true(self):
        async with DatabaseMixin() as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = [v async for v in database.submit_query_and_iter("SELECT * FROM foo;", streaming_mode=True)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_locked(self):
        async with DatabaseMixin() as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = [v async for v in database.submit_query_and_iter("SELECT * FROM foo;", lock=1234)]

        self.assertEqual([(3,), (4,), (5,)], observed)


class TestPostgreSqlMinosDatabase(CommonTestCase, PostgresAsyncTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlMinosDatabase, DatabaseMixin))

    def test_warnings(self):
        pool = DatabaseClientPool.from_config(self.config)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            database = PostgreSqlMinosDatabase(pool)
            self.assertIsInstance(database, DatabaseMixin)


if __name__ == "__main__":
    unittest.main()
