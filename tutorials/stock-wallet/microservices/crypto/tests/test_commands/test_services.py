import sys
import unittest

import pendulum
from src import (
    Crypto,
    CryptoCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestCryptoCommandService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):

        service = CryptoCommandService()
        self.assertIsInstance(service, CryptoCommandService)

    async def test_remote_crypto(self):
        now = pendulum.now()
        now_minus_one_month = now.subtract(months=1)
        service = CryptoCommandService()
        response = service.call_remote("BTC/USD", now_minus_one_month.to_datetime_string())
        self.assertIsInstance(service, CryptoCommandService)


if __name__ == "__main__":
    unittest.main()
