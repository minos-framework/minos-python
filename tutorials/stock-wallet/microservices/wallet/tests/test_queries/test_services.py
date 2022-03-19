import sys
import unittest


from src import (
    WalletQueryService,
)

from tests.utils import (
    build_dependency_injector,
)


class TestWalletQueryService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = WalletQueryService()
        self.assertIsInstance(service, WalletQueryService)

    # async def test_query_add_wallet(self):
    #     service = WalletQueryService()
    #     self.assertIsInstance(service, WalletQueryService)
    #     wallet_event = Event.from_root_entity(Wallet(name="Test Wallet"), action=Action.UPDATE)
    #     wallet_request = InMemoryRequest(wallet_event)
    #     await service.wallet_created(wallet_request)
    #     # get the wallets
    #     response_wallets = await service.get_wallets()
    #     self.assertIsInstance(response_wallets, Response)
    #     response_content = await response_wallets.content()


if __name__ == "__main__":
    unittest.main()
