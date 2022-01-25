import typing as t
import unittest
from abc import (
    ABC,
)

from aiomisc import (
    PoolBase,
)
from aiomisc.pool import (
    T,
)

from minos.common import (
    MinosPool,
    MinosSetup,
)


class _Pool(MinosPool):
    def __init__(self):
        super().__init__()
        self.create_instance_call_count = 0
        self.destroy_instance_call_count = 0

    async def _create_instance(self) -> T:
        self.create_instance_call_count += 1
        return "foo"

    async def _destroy_instance(self, instance: t.Any) -> None:
        self.destroy_instance_call_count += 1


class TestMinosPool(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(MinosPool, (ABC, MinosSetup, PoolBase)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_destroy_instance", "_create_instance"}, MinosPool.__abstractmethods__)

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


if __name__ == "__main__":
    unittest.main()
