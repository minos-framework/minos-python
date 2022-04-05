import unittest
from unittest.mock import (
    PropertyMock,
    call,
    patch,
)

import aiopg
from aiopg import (
    Connection,
    Cursor,
)
from psycopg2 import (
    IntegrityError,
    OperationalError,
)

from minos.common import (
    AiopgDatabaseClient,
    DatabaseLock,
    IntegrityException,
    UnableToConnectException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CommonTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestAiopgDatabaseClient(CommonTestCase, PostgresAsyncTestCase):
    def setUp(self):
        super().setUp()
        self.sql = "SELECT * FROM information_schema.tables"

    def test_constructor(self):
        client = AiopgDatabaseClient("foo")
        self.assertEqual("foo", client.database)
        self.assertEqual("postgres", client.user)
        self.assertEqual("", client.password)
        self.assertEqual("localhost", client.host)
        self.assertEqual(5432, client.port)

    def test_from_config(self):
        default_database = self.config.get_default_database()
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertEqual(default_database["database"], client.database)
        self.assertEqual(default_database["user"], client.user)
        self.assertEqual(default_database["password"], client.password)
        self.assertEqual(default_database["host"], client.host)
        self.assertEqual(default_database["port"], client.port)

    async def test_is_valid_true(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            self.assertTrue(client.is_valid())

    async def test_is_valid_false_not_setup(self):
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertFalse(await client.is_valid())

    async def test_is_valid_false_operational_error(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Connection, "isolation_level", new_callable=PropertyMock, side_effect=OperationalError):
                self.assertFalse(await client.is_valid())

    async def test_is_valid_false_closed(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Connection, "closed", new_callable=PropertyMock, return_valud=False):
                self.assertFalse(await client.is_valid())

    async def test_connection(self):
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertIsNone(client.connection)
        async with client:
            self.assertIsInstance(client.connection, Connection)
        self.assertIsNone(client.connection)

    async def test_connection_raises(self):
        with patch.object(aiopg, "connect", new_callable=PropertyMock, side_effect=OperationalError):
            with self.assertRaises(UnableToConnectException):
                async with AiopgDatabaseClient.from_config(self.config):
                    pass

    async def test_notifications(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            self.assertEqual(client.connection.notifies, client.notifications)

    async def test_cursor(self):
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertIsNone(client.cursor)
        async with client:
            self.assertIsNone(client.cursor)
            await client.execute("SELECT * FROM information_schema.tables")
            self.assertIsInstance(client.cursor, Cursor)

        self.assertIsNone(client.cursor)

    async def test_cursor_reset(self):
        client = AiopgDatabaseClient.from_config(self.config)
        async with client:
            await client.execute("SELECT * FROM information_schema.tables")
            self.assertIsInstance(client.cursor, Cursor)
            await client.reset()
            self.assertIsNone(client.cursor)

    async def test_lock(self):
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertIsNone(client.lock)
        async with client:
            self.assertIsNone(client.lock)
            await client.execute(self.sql, lock="foo")
            self.assertIsInstance(client.lock, DatabaseLock)

        self.assertIsNone(client.lock)

    async def test_lock_reset(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.sql, lock="foo")
            self.assertIsInstance(client.lock, DatabaseLock)
            await client.reset()
            self.assertIsNone(client.lock)

    async def test_execute(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Cursor, "execute") as execute_mock:
                await client.execute(self.sql)
        self.assertEqual(
            [call(operation=self.sql, parameters=None, timeout=None)],
            execute_mock.call_args_list,
        )

    async def test_execute_with_lock(self):
        with patch.object(DatabaseLock, "__aenter__") as enter_lock_mock:
            with patch.object(DatabaseLock, "__aexit__") as exit_lock_mock:
                async with AiopgDatabaseClient.from_config(self.config) as client:
                    await client.execute(self.sql, lock="foo")
                    self.assertEqual(1, enter_lock_mock.call_count)
                    self.assertEqual(0, exit_lock_mock.call_count)
                    enter_lock_mock.reset_mock()
                    exit_lock_mock.reset_mock()
            self.assertEqual(0, enter_lock_mock.call_count)
            self.assertEqual(1, exit_lock_mock.call_count)

    async def test_execute_with_lock_multiple(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.sql, lock="foo")
            await client.execute(self.sql, lock="foo")
            with self.assertRaises(ValueError):
                await client.execute(self.sql, lock="bar")

    async def test_execute_raises_integrity(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Cursor, "execute", side_effect=IntegrityError):
                with self.assertRaises(IntegrityException):
                    await client.execute(self.sql)

    async def test_fetch_one(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.sql)
            observed = await client.fetch_one()
        self.assertIsInstance(observed, tuple)

    async def test_fetch_all(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.sql)
            observed = [value async for value in client.fetch_all()]

        self.assertGreater(len(observed), 0)
        for obs in observed:
            self.assertIsInstance(obs, tuple)


if __name__ == "__main__":
    unittest.main()
