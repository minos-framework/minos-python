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
    ProgrammingError,
)
from psycopg2.extras import (
    DictRow,
)

from minos.common import (
    ConnectionException,
    DatabaseOperation,
    IntegrityException,
    ProgrammingException,
)
from minos.plugins.aiopg import (
    AiopgDatabaseClient,
    AiopgDatabaseOperation,
)
from tests.utils import (
    AiopgTestCase,
)


# noinspection SqlNoDataSourceInspection,SqlDialectInspection
class TestAiopgDatabaseClient(AiopgTestCase):
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

    async def test_is_valid(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            self.assertTrue(await client.is_valid())

    async def test_is_connected_true(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            self.assertTrue(await client.is_connected())

    async def test_is_connected_false_not_setup(self):
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertFalse(await client.is_connected())

    async def test_is_connected_false_operational_error(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Connection, "isolation_level", new_callable=PropertyMock, side_effect=OperationalError):
                self.assertFalse(await client.is_connected())

    async def test_is_connected_false_closed(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Connection, "closed", new_callable=PropertyMock, return_valud=False):
                self.assertFalse(await client.is_connected())

    async def test_connection(self):
        client = AiopgDatabaseClient.from_config(self.config)
        self.assertIsNone(client.connection)
        async with client:
            self.assertIsInstance(client.connection, Connection)
        self.assertIsNone(client.connection)

    async def test_connection_with_circuit_breaker(self):
        async with AiopgDatabaseClient.from_config(self.config) as c1:

            async def _fn():
                return c1.connection

            with patch.object(aiopg, "connect", new_callable=PropertyMock, side_effect=(OperationalError, _fn())):
                async with AiopgDatabaseClient.from_config(self.config) as c2:
                    self.assertEqual(c1.connection, c2.connection)

    async def test_connection_recreate(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            c1 = client.connection
            self.assertIsInstance(c1, Connection)

            await client.recreate()

            c2 = client.connection
            self.assertIsInstance(c2, Connection)

            self.assertNotEqual(c1, c2)

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

    async def test_execute(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Cursor, "execute") as execute_mock:
                await client.execute(self.operation)
        self.assertEqual(
            [call(operation=self.operation.query, parameters=self.operation.parameters)],
            execute_mock.call_args_list,
        )

    async def test_execute_disconnected(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.close()
            self.assertFalse(await client.is_connected())

            await client.execute(self.operation)
            self.assertTrue(await client.is_connected())

    async def test_execute_raises_unsupported(self):
        class _DatabaseOperation(DatabaseOperation):
            """For testing purposes."""

        async with AiopgDatabaseClient.from_config(self.config) as client:
            with self.assertRaises(ValueError):
                await client.execute(_DatabaseOperation())

    async def test_execute_raises_integrity(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Cursor, "execute", side_effect=IntegrityError):
                with self.assertRaises(IntegrityException):
                    await client.execute(self.operation)

    async def test_execute_raises_operational(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with patch.object(Cursor, "execute", side_effect=(OperationalError, None)) as mock:
                await client.execute(self.operation)

        self.assertEqual(
            [
                call(operation=self.operation.query, parameters=self.operation.parameters),
                call(operation=self.operation.query, parameters=self.operation.parameters),
            ],
            mock.call_args_list,
        )

    async def test_fetch_one(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            observed = await client.fetch_one()
        self.assertIsInstance(observed, DictRow)

    async def test_fetch_one_raises_programming_empty(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            with self.assertRaises(ProgrammingException):
                await client.fetch_one()

    async def test_fetch_one_raises_programming(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            with patch.object(Cursor, "fetchone", side_effect=ProgrammingError):
                with self.assertRaises(ProgrammingException):
                    await client.fetch_one()

    async def test_fetch_one_raises_operational(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            with patch.object(Cursor, "fetchone", side_effect=OperationalError):
                with self.assertRaises(ConnectionException):
                    await client.fetch_one()

    async def test_fetch_all(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            observed = [value async for value in client.fetch_all()]

        self.assertGreater(len(observed), 0)
        for obs in observed:
            self.assertIsInstance(obs, DictRow)


if __name__ == "__main__":
    unittest.main()
