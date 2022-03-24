import sys
import unittest

import pendulum

from src import (
    Stocks,
    StocksCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestStocksCommandService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = StocksCommandService()
        self.assertIsInstance(service, StocksCommandService)

    async def test_get_remote_quotes(self):
        service = StocksCommandService()
        now = pendulum.now()
        now_minus_one_month = now.subtract(months=1)
        response = service.call_remote("AAPL", now_minus_one_month.to_date_string(), now.to_date_string())
        self.assertIsInstance(response, list)



if __name__ == "__main__":
    unittest.main()
