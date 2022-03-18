import sys
import unittest

from src import (
    Wallet,
    WalletCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestWalletCommandService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = WalletCommandService()
        self.assertIsInstance(service, WalletCommandService)

    async def test_create_wallet(self):
        service = WalletCommandService()

        request = InMemoryRequest({"wallet_name": "Personal Wallet"})
        response = await service.create_wallet(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()

        self.assertEqual("Personal Wallet", observed["wallet_name"])

    async def test_add_ticker(self):
        service = WalletCommandService()

        request = InMemoryRequest({"wallet_name": "Ticker Wallet Test"})
        response = await service.create_wallet(request)
        self.assertIsInstance(response, Response)
        observed = await response.content()
        self.assertEqual("Ticker Wallet Test", observed["wallet_name"])
        wallet_uuid = observed["uuid"]
        ticker = "AAPL"
        ticker_request = InMemoryRequest({"wallet": wallet_uuid, "ticker": ticker})
        ticker_response = await service.add_ticker(ticker_request)
        self.assertIsInstance(ticker_response, Response)
        ticker_observed = await ticker_response.content()
        self.assertEqual("AAPL", ticker_observed["ticker"])


if __name__ == "__main__":
    unittest.main()
