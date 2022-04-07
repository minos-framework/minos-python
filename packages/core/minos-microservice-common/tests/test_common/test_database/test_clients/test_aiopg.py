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
    AiopgDatabaseOperation,
    DatabaseLock,
    IntegrityException,
    UnableToConnectException,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from tests.utils import (
    CommonTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestAiopgDatabaseClient(CommonTestCase, DatabaseMinosTestCase):
    def setUp(self):
        super().setUp()
        self.operation = AiopgDatabaseOperation("SELECT * FROM information_schema.tables")

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
            self.assertTrue(await client.is_valid())

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

    async def test_cursor(self):
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertIsNone(client.cursor)
        async with client:
            self.assertIsNone(client.cursor)
            await client.execute(self.operation)
            self.assertIsInstance(client.cursor, Cursor)

        self.assertIsNone(client.cursor)

    async def test_cursor_reset(self):
        client = AiopgDatabaseClient.from_config(self.config)
        async with client:
            await client.execute(self.operation)
            self.assertIsInstance(client.cursor, Cursor)
            await client.reset()
            self.assertIsNone(client.cursor)

    async def test_lock(self):
        op1 = AiopgDatabaseOperation("SELECT * FROM information_schema.tables", lock="foo")
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertIsNone(client.lock)
        async with client:
            self.assertIsNone(client.lock)
            await client.execute(op1)
            self.assertIsInstance(client.lock, DatabaseLock)

        self.assertIsNone(client.lock)

    async def test_lock_reset(self):
        op1 = AiopgDatabaseOperation("SELECT * FROM information_schema.tables", lock="foo")
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(op1)
            self.assertIsInstance(client.lock, DatabaseLock)
            await client.reset()
            self.assertIsNone(client.lock)

    async def test_execute(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Cursor, "execute") as execute_mock:
                await client.execute(self.operation)
        self.assertEqual(
            [call(operation=self.operation.query, parameters=self.operation.parameters)],
            execute_mock.call_args_list,
        )

    async def test_execute_with_lock(self):
        op1 = AiopgDatabaseOperation("SELECT * FROM information_schema.tables", lock="foo")
        with patch.object(DatabaseLock, "acquire") as enter_lock_mock:
            with patch.object(DatabaseLock, "release") as exit_lock_mock:
                async with AiopgDatabaseClient.from_config(self.config) as client:
                    await client.execute(op1)
                    self.assertEqual(1, enter_lock_mock.call_count)
                    self.assertEqual(0, exit_lock_mock.call_count)
                    enter_lock_mock.reset_mock()
                    exit_lock_mock.reset_mock()
            self.assertEqual(0, enter_lock_mock.call_count)
            self.assertEqual(1, exit_lock_mock.call_count)

    async def test_execute_with_lock_multiple(self):
        op1 = AiopgDatabaseOperation("SELECT * FROM information_schema.tables", lock="foo")
        op2 = AiopgDatabaseOperation("SELECT * FROM information_schema.tables", lock="bar")
        async with AiopgDatabaseClient.from_config(self.config) as client:
            self.assertIsNone(client.lock)

            await client.execute(op1)
            foo_lock = client.lock
            self.assertIsInstance(foo_lock, DatabaseLock)

            await client.execute(op1)
            self.assertEqual(foo_lock, client.lock)

            await client.execute(op2)
            self.assertNotEqual(foo_lock, client.lock)
            self.assertIsInstance(client.lock, DatabaseLock)

    async def test_execute_raises_integrity(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Cursor, "execute", side_effect=IntegrityError):
                with self.assertRaises(IntegrityException):
                    await client.execute(self.operation)

    async def test_fetch_one(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            observed = await client.fetch_one()
        self.assertIsInstance(observed, tuple)

    async def test_fetch_all(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            observed = [value async for value in client.fetch_all()]

        self.assertGreater(len(observed), 0)
        for obs in observed:
            self.assertIsInstance(obs, tuple)


if __name__ == "__main__":
    unittest.main()
