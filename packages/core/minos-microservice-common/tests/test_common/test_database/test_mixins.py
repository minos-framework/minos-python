import unittest

from minos.common import (
    DatabaseClientPool,
    DatabaseMixin,
    LockDatabaseOperationFactory,
    NotProvidedException,
    PoolFactory,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
    MockedDatabaseClient,
    MockedDatabaseOperation,
    MockedLockDatabaseOperationFactory,
)
from tests.utils import (
    CommonTestCase,
)


# noinspection SqlNoDataSourceInspection,SqlResolve
class TestDatabaseMixin(CommonTestCase, DatabaseMinosTestCase):
    async def test_constructor(self):
        async with DatabaseClientPool.from_config(self.config) as pool:
            # noinspection PyTypeChecker
            database = DatabaseMixin(pool)
            self.assertEqual(pool, database.database_pool)

    async def test_constructor_from_config(self):
        async with DatabaseClientPool.from_config(self.config) as pool:
            # noinspection PyTypeChecker
            database = DatabaseMixin.from_config(self.config, database_pool=pool)
            self.assertEqual(pool, database.database_pool)

    async def test_constructor_with_pool_factory(self):
        async with PoolFactory(self.config, {"database": DatabaseClientPool}) as pool_factory:
            # noinspection PyTypeChecker
            database = DatabaseMixin(pool_factory=pool_factory)
            # noinspection PyUnresolvedReferences
            self.assertEqual(pool_factory.get_pool("database"), database.database_pool)

    async def test_constructor_with_pool_factory_and_database_key(self):
        async with PoolFactory(self.config, {"database": DatabaseClientPool}) as pool_factory:
            # noinspection PyTypeChecker
            database = DatabaseMixin(pool_factory=pool_factory, database_key=("query", "unknown"))
            # noinspection PyUnresolvedReferences
            self.assertEqual(pool_factory.get_pool("database", "query"), database.database_pool)

    async def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyArgumentEqualDefault
            DatabaseMixin(database_pool=None, pool_factory=None)

    async def test_pool(self):
        async with DatabaseMixin() as database:
            self.assertIsInstance(database.database_pool, DatabaseClientPool)

    async def test_operation_factory(self):
        operation_factory = MockedLockDatabaseOperationFactory()
        mixin = DatabaseMixin(operation_factory=operation_factory)
        self.assertEqual(operation_factory, mixin.database_operation_factory)

    async def test_operation_factory_from_cls_init(self):
        mixin = DatabaseMixin(operation_factory_cls=LockDatabaseOperationFactory)
        self.assertIsInstance(mixin.database_operation_factory, MockedLockDatabaseOperationFactory)

    async def test_operation_factory_from_cls_generic(self):
        class _DatabaseMixin(DatabaseMixin[LockDatabaseOperationFactory]):
            """For testing purposes."""

        mixin = _DatabaseMixin()
        self.assertIsInstance(mixin.database_operation_factory, MockedLockDatabaseOperationFactory)

    async def test_operation_factory_none(self):
        mixin = DatabaseMixin()
        self.assertEqual(None, mixin.database_operation_factory)

    async def test_operation_factory_from_cls_generic_raises(self):
        class _DatabaseMixin(DatabaseMixin[int]):
            """For testing purposes."""

        with self.assertRaises(TypeError):
            _DatabaseMixin()

    async def test_execute_on_database(self):
        op1 = MockedDatabaseOperation("create_table")
        op2 = MockedDatabaseOperation("check_exist", [(True,)])

        async with DatabaseMixin() as database:
            await database.execute_on_database(op1)

        async with MockedDatabaseClient(**self.config.get_default_database()) as client:
            await client.execute(op2)
            self.assertTrue((await client.fetch_one())[0])

    async def test_execute_on_database_locked(self):
        op1 = MockedDatabaseOperation("create_table", lock=1234)
        op2 = MockedDatabaseOperation("check_exist", [(True,)])

        async with DatabaseMixin() as database:
            await database.execute_on_database(op1)

        async with MockedDatabaseClient(**self.config.get_default_database()) as client:
            await client.execute(op2)
            self.assertTrue((await client.fetch_one())[0])

    async def test_execute_on_database_and_fetch_one(self):
        op1 = MockedDatabaseOperation("create_table")
        op2 = MockedDatabaseOperation("insert")
        op3 = MockedDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.execute_on_database(op1)
            await database.execute_on_database(op2)

            observed = await database.execute_on_database_and_fetch_one(op3)

        self.assertEqual((3,), observed)

    async def test_execute_on_database_and_fetch_all(self):
        op1 = MockedDatabaseOperation("create_table")
        op2 = MockedDatabaseOperation("insert")
        op3 = MockedDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.execute_on_database(op1)
            await database.execute_on_database(op2)
            observed = [v async for v in database.execute_on_database_and_fetch_all(op3)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_execute_on_database_and_fetch_all_streaming_mode_true(self):
        op1 = MockedDatabaseOperation("create_table")
        op2 = MockedDatabaseOperation("insert")
        op3 = MockedDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.execute_on_database(op1)
            await database.execute_on_database(op2)

            observed = [v async for v in database.execute_on_database_and_fetch_all(op3, streaming_mode=True)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_execute_on_database_and_fetch_all_locked(self):
        op1 = MockedDatabaseOperation("create_table", lock=1234)
        op2 = MockedDatabaseOperation("insert")
        op3 = MockedDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.execute_on_database(op1)
            await database.execute_on_database(op2)

            observed = [v async for v in database.execute_on_database_and_fetch_all(op3)]

        self.assertEqual([(3,), (4,), (5,)], observed)


if __name__ == "__main__":
    unittest.main()
