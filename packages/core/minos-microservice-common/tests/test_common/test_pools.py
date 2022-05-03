import typing as t
import unittest
import warnings
from asyncio import (
    gather,
    sleep,
)
from typing import (
    Any,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from aiomisc import (
    PoolBase,
)

from minos.common import (
    MinosConfigException,
    MinosPool,
    Pool,
    PoolException,
    PoolFactory,
    SetupMixin,
)
from tests.utils import (
    CommonTestCase,
    FakeLockPool,
)


class TestPoolFactory(CommonTestCase):
    def setUp(self):
        super().setUp()
        self.factory = PoolFactory(self.config, {"lock": FakeLockPool})

    def test_from_config(self):
        self.assertIsInstance(PoolFactory.from_config(self.config), PoolFactory)

    async def asyncTearDown(self):
        await self.factory.destroy()
        await super().asyncTearDown()

    def test_get_pool(self):
        lock = self.factory.get_pool("lock")
        self.assertIsInstance(lock, FakeLockPool)
        self.assertEqual(lock, self.factory.get_pool("lock"))

    def test_get_pool_with_key(self):
        lock_a = self.factory.get_pool("lock", "a")
        lock_b = self.factory.get_pool("lock", "b")
        self.assertIsInstance(lock_a, FakeLockPool)
        self.assertIsInstance(lock_b, FakeLockPool)

        self.assertNotEqual(lock_a, lock_b)
        self.assertEqual(lock_a, self.factory.get_pool("lock", "a"))
        self.assertEqual(lock_b, self.factory.get_pool("lock", "b"))

    def test_get_pool_cls_raises(self):
        with self.assertRaises(PoolException):
            self.factory.get_pool("something")

    def test_get_pool_identifier_raises(self):
        with patch.object(SetupMixin, "from_config", side_effect=MinosConfigException("")):
            with self.assertRaises(PoolException):
                self.factory.get_pool("database")


class TestPool(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(Pool, (SetupMixin, PoolBase)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_destroy_instance", "_create_instance"}, Pool.__abstractmethods__)

    async def test_acquire(self):
        async with _Pool() as pool:
            self.assertEqual(0, pool.create_instance_call_count)
            self.assertEqual(0, pool.destroy_instance_call_count)
            async with pool.acquire() as observed:
                self.assertEqual(1, pool.create_instance_call_count)
                self.assertEqual(0, pool.destroy_instance_call_count)
                self.assertEqual("foo", observed)
        self.assertEqual(1, pool.create_instance_call_count)
        self.assertLess(0, pool.destroy_instance_call_count)

    async def test_close(self):
        async def _fn1(p):
            async with p.acquire():
                await sleep(0.5)

        async def _fn2(p):
            await p.destroy()

        async with _Pool() as pool:
            pool_mock = MagicMock(side_effect=pool.close)
            pool.close = pool_mock
            await gather(_fn1(pool), _fn2(pool))

        self.assertEqual(1, pool_mock.call_count)


class _Pool(Pool):
    def __init__(self):
        super().__init__()
        self.create_instance_call_count = 0
        self.destroy_instance_call_count = 0

    async def _create_instance(self):
        self.create_instance_call_count += 1
        return "foo"

    async def _destroy_instance(self, instance: t.Any) -> None:
        self.destroy_instance_call_count += 1


class TestMinosPool(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(MinosPool, SetupMixin))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            setup = _MinosPool()
            self.assertIsInstance(setup, SetupMixin)


class _MinosPool(MinosPool):
    """For testing purposes."""

    async def _create_instance(self):
        """For testing purposes."""

    async def _destroy_instance(self, instance: Any) -> None:
        """For testing purposes."""


if __name__ == "__main__":
    unittest.main()
