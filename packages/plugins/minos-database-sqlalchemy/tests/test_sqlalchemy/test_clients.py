import unittest
from unittest.mock import (
    PropertyMock,
    call,
    patch,
)

from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    insert,
    select,
)
from sqlalchemy.engine import (
    URL,
    Row,
)
from sqlalchemy.exc import (
    IntegrityError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncResult,
)

from minos.common import (
    ConnectionException,
    DatabaseOperation,
    IntegrityException,
    ProgrammingException,
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


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class TestSqlAlchemyDatabaseClient(SqlAlchemyTestCase):
    def setUp(self):
        super().setUp()
        self.operation = SqlAlchemyDatabaseOperation("SELECT * FROM information_schema.tables")

    def test_constructor(self):
        client = SqlAlchemyDatabaseClient(database="foo", driver="postgresql+asyncpg")
        expected = URL.create(database="foo", drivername="postgresql+asyncpg")
        self.assertEqual(expected, client.url)

    def test_from_config(self):
        client = SqlAlchemyDatabaseClient.from_config(self.config)
        database_config = self.config.get_default_database()
        expected = URL.create(
            drivername=database_config["driver"],
            username=database_config["user"],
            password=database_config["password"],
            host=database_config["host"],
            port=database_config["port"],
            database=database_config["database"],
        )
        self.assertEqual(expected, client.url)

    async def test_is_valid(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            self.assertTrue(await client.is_valid())

    async def test_is_connected_true(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            self.assertTrue(await client.is_connected())

    async def test_is_connected_false_not_setup(self):
        client = SqlAlchemyDatabaseClient.from_config(self.config)
        self.assertFalse(await client.is_connected())

    async def test_is_connected_false_closed(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            with patch.object(AsyncConnection, "closed", new_callable=PropertyMock, return_valud=False):
                self.assertFalse(await client.is_connected())

    async def test_connection(self):
        client = SqlAlchemyDatabaseClient.from_config(self.config)
        self.assertIsNone(client.connection)
        async with client:
            self.assertIsInstance(client.connection, AsyncConnection)
        self.assertIsNone(client.connection)

    async def test_connection_with_circuit_breaker(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as c1:

            async def _fn():
                return c1.connection

            with patch.object(
                AsyncEngine,
                "connect",
                side_effect=(OperationalError("", dict(), None), _fn()),
            ):
                async with SqlAlchemyDatabaseClient.from_config(self.config) as c2:
                    self.assertEqual(c1.connection, c2.connection)

    async def test_connection_recreate(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            c1 = client.connection
            self.assertIsInstance(c1, AsyncConnection)

            await client.recreate()

            c2 = client.connection
            self.assertIsInstance(c2, AsyncConnection)

            self.assertNotEqual(c1, c2)

    async def test_cursor(self):
        client = SqlAlchemyDatabaseClient.from_config(self.config)
        self.assertIsNone(client.result)
        async with client:
            self.assertIsNone(client.result)
            await client.execute(self.operation)
            self.assertIsInstance(client.result, AsyncResult)

        self.assertIsNone(client.result)

    async def test_cursor_reset(self):
        client = SqlAlchemyDatabaseClient.from_config(self.config)
        async with client:
            await client.execute(self.operation)
            self.assertIsInstance(client.result, AsyncResult)
            await client.reset()
            self.assertIsNone(client.result)

    async def test_execute_callable(self):
        operation = SqlAlchemyDatabaseOperation(meta.create_all)
        async with SqlAlchemyDatabaseClient(**self.config.get_default_database()) as client:
            with patch.object(AsyncConnection, "run_sync") as execute_mock:
                await client.execute(operation)
            self.assertIsNone(client.result)
        self.assertEqual([call(meta.create_all)], execute_mock.call_args_list)

    async def test_execute_clause(self):
        sentinel = object()
        operation = SqlAlchemyDatabaseOperation(insert(foo).values([{"bar": "one"}, {"bar": "two"}, {"bar": "three"}]))
        async with SqlAlchemyDatabaseClient(**self.config.get_default_database()) as client:
            with patch.object(AsyncConnection, "stream", return_value=sentinel) as execute_mock:
                await client.execute(operation)
            self.assertEqual(sentinel, client.result)

        self.assertEqual(
            [call(statement=operation.statement, parameters=operation.parameters)], execute_mock.call_args_list
        )

    async def test_execute_clause_without_stream(self):
        operation = SqlAlchemyDatabaseOperation(
            insert(foo).values([{"bar": "one"}, {"bar": "two"}, {"bar": "three"}]), stream=False
        )
        async with SqlAlchemyDatabaseClient(**self.config.get_default_database()) as client:
            with patch.object(AsyncConnection, "execute") as execute_mock:
                await client.execute(operation)
            self.assertIsNone(client.result)

        self.assertEqual(
            [call(statement=operation.statement, parameters=operation.parameters)], execute_mock.call_args_list
        )

    async def test_execute_str(self):
        sentinel = object()
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            with patch.object(AsyncConnection, "stream", return_value=sentinel) as execute_mock:
                await client.execute(self.operation)
            self.assertEqual(sentinel, client.result)
        self.assertEqual(
            [call(statement=self.operation.statement, parameters=self.operation.parameters)],
            execute_mock.call_args_list,
        )

    async def test_execute_disconnected(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            await client.close()
            self.assertFalse(await client.is_connected())

            await client.execute(self.operation)
            self.assertTrue(await client.is_connected())

    async def test_execute_raises_unsupported(self):
        class _DatabaseOperation(DatabaseOperation):
            """For testing purposes."""

        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            with self.assertRaises(ValueError):
                await client.execute(_DatabaseOperation())

    async def test_execute_raises_integrity(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            with patch.object(AsyncConnection, "stream", side_effect=IntegrityError("", dict(), None)):
                with self.assertRaises(IntegrityException):
                    await client.execute(self.operation)

    async def test_execute_raises_operational(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            with patch.object(
                AsyncConnection, "stream", side_effect=(OperationalError("", dict(), None), None)
            ) as mock:
                await client.execute(self.operation)

        self.assertEqual(
            [
                call(statement=self.operation.statement, parameters=self.operation.parameters),
                call(statement=self.operation.statement, parameters=self.operation.parameters),
            ],
            mock.call_args_list,
        )

    async def test_execute_raises_programming(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            with patch.object(AsyncConnection, "stream", side_effect=ProgrammingError("", dict(), None)):
                with self.assertRaises(ProgrammingException):
                    await client.execute(self.operation)

    async def test_fetch_one(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            observed = await client.fetch_one()
        self.assertIsInstance(observed, Row)

    async def test_fetch_one_raises_programming_empty(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            with self.assertRaises(ProgrammingException):
                await client.fetch_one()

    async def test_fetch_one_raises_programming(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            with patch.object(AsyncResult, "__anext__", side_effect=ProgrammingError("", dict(), None)):
                with self.assertRaises(ProgrammingException):
                    await client.fetch_one()

    async def test_fetch_one_raises_operational(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            with patch.object(AsyncResult, "__anext__", side_effect=OperationalError("", dict(), None)):
                with self.assertRaises(ConnectionException):
                    await client.fetch_one()

    async def test_fetch_all(self):
        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            await client.execute(self.operation)
            observed = [value async for value in client.fetch_all()]

        self.assertGreater(len(observed), 0)
        for obs in observed:
            self.assertIsInstance(obs, Row)

    async def test_integration_test(self):
        async with SqlAlchemyDatabaseClient(**self.config.get_default_database()) as client:
            operation = SqlAlchemyDatabaseOperation(meta.create_all)
            await client.execute(operation)

            operation = SqlAlchemyDatabaseOperation(
                insert(foo).values([{"bar": "one"}, {"bar": "two"}, {"bar": "three"}])
            )
            await client.execute(operation)

            operation = SqlAlchemyDatabaseOperation(select(foo))
            await client.execute(operation)

            expected = [{"bar": "one"}, {"bar": "two"}, {"bar": "three"}]

            observed = list()
            async for row in client.fetch_all():
                observed.append(dict(row))

            self.assertEqual(expected, observed)

            await client.recreate()  # Necessary to delete table

            operation = SqlAlchemyDatabaseOperation(meta.drop_all)
            await client.execute(operation)


if __name__ == "__main__":
    unittest.main()
