import unittest
from abc import (
    ABC,
)
from typing import (
    Any,
    AsyncIterator,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
    patch,
)

from minos.common import (
    BuildableMixin,
    ComposedDatabaseOperation,
    DatabaseClient,
    DatabaseClientBuilder,
    DatabaseLock,
    DatabaseOperation,
    DatabaseOperationFactory,
    LockDatabaseOperationFactory,
    ProgrammingException,
)
from tests.utils import (
    CommonTestCase,
    FakeAsyncIterator,
)


class _DatabaseClient(DatabaseClient):
    """For testing purposes."""

    async def _reset(self, **kwargs) -> None:
        """For testing purposes."""

    async def _execute(self, *args, **kwargs) -> None:
        """For testing purposes."""

    def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        """For testing purposes."""


class _DatabaseOperation(DatabaseOperation):
    """For testing purposes."""


class _DatabaseOperationFactory(DatabaseOperationFactory):
    """For testing purposes."""


class _DatabaseOperationFactoryImpl(_DatabaseOperationFactory):
    """For testing purposes."""


class _LockDatabaseOperationFactory(LockDatabaseOperationFactory):
    """For testing purposes."""

    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes."""
        return _DatabaseOperation()

    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes."""
        return _DatabaseOperation()


_DatabaseClient.set_factory(LockDatabaseOperationFactory, _LockDatabaseOperationFactory)


class TestDatabaseClient(CommonTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(DatabaseClient, (ABC, BuildableMixin)))
        expected = {"_execute", "_fetch_all", "_reset"}
        # noinspection PyUnresolvedReferences
        self.assertEqual(expected, DatabaseClient.__abstractmethods__)

    def test_get_builder(self):
        self.assertIsInstance(DatabaseClient.get_builder(), DatabaseClientBuilder)

    def test_from_config(self):
        client = _DatabaseClient.from_config(self.config)
        self.assertIsInstance(client, DatabaseClient)

    async def test_is_valid(self):
        mock = AsyncMock(side_effect=[True, False])
        client = _DatabaseClient()
        client._is_valid = mock

        self.assertEqual(True, await client.is_valid())
        self.assertEqual(False, await client.is_valid())

        self.assertEqual([call(), call()], mock.call_args_list)

    async def test_lock(self):
        _DatabaseClient.set_factory(LockDatabaseOperationFactory, _LockDatabaseOperationFactory)
        op1 = _DatabaseOperation(lock="foo")
        client = _DatabaseClient()
        self.assertIsNone(client.lock)
        async with client:
            self.assertIsNone(client.lock)
            await client.execute(op1)
            self.assertIsInstance(client.lock, DatabaseLock)

        self.assertIsNone(client.lock)

    async def test_lock_reset(self):
        op1 = _DatabaseOperation(lock="foo")
        async with _DatabaseClient() as client:
            await client.execute(op1)
            self.assertIsInstance(client.lock, DatabaseLock)
            await client.reset()
            self.assertIsNone(client.lock)

    async def test_reset(self):
        mock = AsyncMock()
        client = _DatabaseClient()
        client._reset = mock

        await client.reset()

        self.assertEqual([call()], mock.call_args_list)

    async def test_execute(self):
        mock = AsyncMock()
        client = _DatabaseClient()
        client._execute = mock
        operation = _DatabaseOperation()
        await client.execute(operation)

        self.assertEqual([call(operation)], mock.call_args_list)

    async def test_execute_composed(self):
        mock = AsyncMock()
        client = _DatabaseClient()
        client._execute = mock
        composed = ComposedDatabaseOperation([_DatabaseOperation(), _DatabaseOperation()])
        await client.execute(composed)

        self.assertEqual([call(composed.operations[0]), call(composed.operations[1])], mock.call_args_list)

    async def test_execute_with_lock(self):
        op1 = _DatabaseOperation(lock="foo")
        with patch.object(DatabaseLock, "acquire") as enter_lock_mock:
            with patch.object(DatabaseLock, "release") as exit_lock_mock:
                async with _DatabaseClient() as client:
                    await client.execute(op1)
                    self.assertEqual(1, enter_lock_mock.call_count)
                    self.assertEqual(0, exit_lock_mock.call_count)
                    enter_lock_mock.reset_mock()
                    exit_lock_mock.reset_mock()
            self.assertEqual(0, enter_lock_mock.call_count)
            self.assertEqual(1, exit_lock_mock.call_count)

    async def test_execute_with_lock_multiple(self):
        op1 = _DatabaseOperation(lock="foo")
        op2 = _DatabaseOperation(lock="bar")
        async with _DatabaseClient() as client:
            self.assertIsNone(client.lock)

            await client.execute(op1)
            foo_lock = client.lock
            self.assertIsInstance(foo_lock, DatabaseLock)

            await client.execute(op1)
            self.assertEqual(foo_lock, client.lock)

            await client.execute(op2)
            self.assertNotEqual(foo_lock, client.lock)
            self.assertIsInstance(client.lock, DatabaseLock)

    async def test_execute_raises_unsupported(self):
        client = _DatabaseClient()
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            await client.execute("wrong!")

    async def test_fetch_all(self):
        mock = MagicMock(return_value=FakeAsyncIterator(["one", "two"]))
        client = _DatabaseClient()
        client._fetch_all = mock

        self.assertEqual(["one", "two"], [v async for v in client.fetch_all()])

        self.assertEqual([call()], mock.call_args_list)

    async def test_fetch_one(self):
        mock = MagicMock(return_value=FakeAsyncIterator(["one", "two"]))
        client = _DatabaseClient()
        client._fetch_all = mock

        self.assertEqual("one", await client.fetch_one())

        self.assertEqual([call()], mock.call_args_list)

    async def test_fetch_one_raises(self):
        mock = MagicMock(return_value=FakeAsyncIterator([]))
        client = _DatabaseClient()
        client._fetch_all = mock

        with self.assertRaises(ProgrammingException):
            await client.fetch_one()

    def test_set_factory(self):
        expected = {
            LockDatabaseOperationFactory: _LockDatabaseOperationFactory,
            _DatabaseOperationFactory: _DatabaseOperationFactoryImpl,
        }
        try:
            _DatabaseClient.set_factory(_DatabaseOperationFactory, _DatabaseOperationFactoryImpl)

            self.assertEqual(expected, _DatabaseClient._factories)
        finally:
            _DatabaseClient._factories.pop(_DatabaseOperationFactory)

    def test_set_factory_raises(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            _DatabaseClient.set_factory(object, DatabaseOperationFactory)

        with self.assertRaises(ValueError):
            _DatabaseClient.set_factory(_DatabaseOperationFactoryImpl, _DatabaseOperationFactory)

    def test_get_factory(self):
        self.assertIsInstance(
            _DatabaseClient.get_factory(LockDatabaseOperationFactory),
            _LockDatabaseOperationFactory,
        )

    def test_get_factory_raises(self):
        with self.assertRaises(ValueError):
            _DatabaseClient.get_factory(_DatabaseOperationFactory),


class TestDatabaseClientBuilder(CommonTestCase):
    def test_with_name(self):
        builder = DatabaseClientBuilder(_DatabaseClient).with_name("query")
        self.assertEqual({"name": "query"}, builder.kwargs)

    def test_with_config(self):
        builder = DatabaseClientBuilder(_DatabaseClient).with_name("query").with_config(self.config)
        self.assertEqual({"name": "query"} | self.config.get_database_by_name("query"), builder.kwargs)


if __name__ == "__main__":
    unittest.main()
