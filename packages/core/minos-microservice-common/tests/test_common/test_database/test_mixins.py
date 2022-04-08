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
)
from tests.utils import (
    CommonTestCase,
    FakeDatabaseClient,
    FakeDatabaseOperation,
    FakeLockDatabaseOperationFactory,
)


# noinspection SqlNoDataSourceInspection,SqlResolve
class TestDatabaseMixin(CommonTestCase, DatabaseMinosTestCase):
    def test_constructor(self):
        pool = DatabaseClientPool.from_config(self.config)
        # noinspection PyTypeChecker
        database = DatabaseMixin(pool)
        self.assertEqual(pool, database.database_pool)

    async def test_constructor_with_pool_factory(self):
        pool_factory = PoolFactory(self.config, {"database": DatabaseClientPool})
        # noinspection PyTypeChecker
        database = DatabaseMixin(pool_factory=pool_factory)
        # noinspection PyUnresolvedReferences
        self.assertEqual(pool_factory.get_pool("database"), database.database_pool)

    async def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyArgumentEqualDefault
            DatabaseMixin(database_pool=None, pool_factory=None)

    async def test_pool(self):
        async with DatabaseMixin() as database:
            self.assertIsInstance(database.database_pool, DatabaseClientPool)

    async def test_operation_factory(self):
        operation_factory = FakeLockDatabaseOperationFactory()
        mixin = DatabaseMixin(operation_factory=operation_factory)
        self.assertEqual(operation_factory, mixin.operation_factory)

    async def test_operation_factory_from_cls_init(self):
        mixin = DatabaseMixin(operation_factory_cls=LockDatabaseOperationFactory)
        self.assertIsInstance(mixin.operation_factory, FakeLockDatabaseOperationFactory)

    async def test_operation_factory_from_cls_generic(self):
        class _DatabaseMixin(DatabaseMixin[LockDatabaseOperationFactory]):
            """For testing purposes."""

        mixin = _DatabaseMixin()
        self.assertIsInstance(mixin.operation_factory, FakeLockDatabaseOperationFactory)

    async def test_operation_factory_none(self):
        mixin = DatabaseMixin()
        self.assertEqual(None, mixin.operation_factory)

    async def test_operation_factory_from_cls_generic_raises(self):
        class _DatabaseMixin(DatabaseMixin[int]):
            """For testing purposes."""

        with self.assertRaises(TypeError):
            _DatabaseMixin()

    async def test_submit_query(self):
        op1 = FakeDatabaseOperation("create_table")
        op2 = FakeDatabaseOperation("check_exist", [(True,)])

        async with DatabaseMixin() as database:
            await database.submit_query(op1)

        async with FakeDatabaseClient(**self.config.get_default_database()) as client:
            await client.execute(op2)
            self.assertTrue((await client.fetch_one())[0])

    async def test_submit_query_locked(self):
        op1 = FakeDatabaseOperation("create_table", lock=1234)
        op2 = FakeDatabaseOperation("check_exist", [(True,)])

        async with DatabaseMixin() as database:
            await database.submit_query(op1)

        async with FakeDatabaseClient(**self.config.get_default_database()) as client:
            await client.execute(op2)
            self.assertTrue((await client.fetch_one())[0])

    async def test_submit_query_and_fetchone(self):
        op1 = FakeDatabaseOperation("create_table")
        op2 = FakeDatabaseOperation("insert")
        op3 = FakeDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)

            observed = await database.submit_query_and_fetchone(op3)

        self.assertEqual((3,), observed)

    async def test_submit_query_and_iter(self):
        op1 = FakeDatabaseOperation("create_table")
        op2 = FakeDatabaseOperation("insert")
        op3 = FakeDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)
            observed = [v async for v in database.submit_query_and_iter(op3)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_streaming_mode_true(self):
        op1 = FakeDatabaseOperation("create_table")
        op2 = FakeDatabaseOperation("insert")
        op3 = FakeDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)

            observed = [v async for v in database.submit_query_and_iter(op3, streaming_mode=True)]

        self.assertEqual([(3,), (4,), (5,)], observed)

    async def test_submit_query_and_iter_locked(self):
        op1 = FakeDatabaseOperation("create_table", lock=1234)
        op2 = FakeDatabaseOperation("insert")
        op3 = FakeDatabaseOperation("select", [(3,), (4,), (5,)])

        async with DatabaseMixin() as database:
            await database.submit_query(op1)
            await database.submit_query(op2)

            observed = [v async for v in database.submit_query_and_iter(op3)]

        self.assertEqual([(3,), (4,), (5,)], observed)


if __name__ == "__main__":
    unittest.main()
