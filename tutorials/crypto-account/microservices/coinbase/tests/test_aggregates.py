import sys
import unittest

from src import (
    Coinbase,
)

from tests.utils import (
    build_dependency_injector,
)


class TestCoinbase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        obj = Coinbase()
        self.assertIsInstance(obj, Coinbase)


if __name__ == '__main__':
    unittest.main()