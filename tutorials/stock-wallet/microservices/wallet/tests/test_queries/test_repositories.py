import sys
import unittest

from minos.aggregate import Event, Action
from minos.networks import InMemoryRequest, Response

from src import (
    WalletQueryServiceRepository, Wallet,
)

from tests.utils import (
    build_dependency_injector,
)


class TestWalletQueryServiceRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = WalletQueryServiceRepository()
        self.assertIsInstance(service, WalletQueryServiceRepository)


if __name__ == "__main__":
    unittest.main()
