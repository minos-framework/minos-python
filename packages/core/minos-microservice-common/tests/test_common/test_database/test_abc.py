import sys
import unittest

import aiopg

from minos.common import (
    DatabaseClientPool,
    DatabaseMixin,
    DependencyInjector,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPostgreSqlMinosDatabase(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_constructor(self):
        pool = DatabaseMixin("foo")
        self.assertEqual("foo", pool.database)
        self.assertEqual("postgres", pool.user)
        self.assertEqual("", pool.password)
        self.assertEqual("localhost", pool.host)
        self.assertEqual(5432, pool.port)

    def test_constructor_extended(self):
        database = DatabaseMixin(**self.repository_db)
        self.assertEqual(self.repository_db["host"], database.host)
        self.assertEqual(self.repository_db["port"], database.port)
        self.assertEqual(self.repository_db["database"], database.database)
        self.assertEqual(self.repository_db["user"], database.user)
        self.assertEqual(self.repository_db["password"], database.password)

    async def test_pool(self):
        async with DatabaseMixin(**self.repository_db) as database:
            self.assertIsInstance(database.pool, DatabaseClientPool)

    async def test_pool_with_dependency_injections(self):
        injector = DependencyInjector(self.config, [DatabaseClientPool])
        await injector.wire_and_setup_injections(modules=[sys.modules[__name__]])

        async with DatabaseMixin(**self.repository_db) as database:
            self.assertEqual(injector.postgresql_pool, database.pool)

        await injector.unwire_and_destroy_injections()

    async def test_submit_query(self):
        async with DatabaseMixin(**self.repository_db) as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")

        async with aiopg.connect(**self.repository_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'foo');")
                self.assertTrue((await cursor.fetchone())[0])

    async def test_submit_query_locked(self):
        async with DatabaseMixin(**self.repository_db) as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);", lock=1234)

        async with aiopg.connect(**self.repository_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'foo');")
                self.assertTrue((await cursor.fetchone())[0])

    async def test_submit_query_and_fetchone(self):
        async with DatabaseMixin(**self.repository_db) as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = await database.submit_query_and_fetchone("SELECT * FROM foo;")

        self.assertEqual((3,), observed)

    async def test_submit_query_and_iter(self):
        async with DatabaseMixin(**self.repository_db) as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = [v async for v in database.submit_query_and_iter("SELECT * FROM foo;")]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_streaming_mode_true(self):
        async with DatabaseMixin(**self.repository_db) as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = [v async for v in database.submit_query_and_iter("SELECT * FROM foo;", streaming_mode=True)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_locked(self):
        async with DatabaseMixin(**self.repository_db) as database:
            await database.submit_query("CREATE TABLE foo (id INT NOT NULL);")
            await database.submit_query("INSERT INTO foo (id) VALUES (3), (4), (5);")

            observed = [v async for v in database.submit_query_and_iter("SELECT * FROM foo;", lock=1234)]

        self.assertEqual([(3,), (4,), (5,)], observed)


if __name__ == "__main__":
    unittest.main()
