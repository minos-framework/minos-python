import sys
import unittest

from src import (
    Stocks,
)

from tests.utils import (
    build_dependency_injector,
)


class TestStocks(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        obj = Stocks()
        self.assertIsInstance(obj, Stocks)


if __name__ == '__main__':
    unittest.main()