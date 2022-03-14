import sys
import unittest

from src import (
    Currency,
    CurrencyCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestCurrencyCommandService(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = CurrencyCommandService()
        self.assertIsInstance(service, CurrencyCommandService)

    async def test_create_currency_quote(self):
        service = CurrencyCommandService()

        request = InMemoryRequest({"quantity": 100.0, "from": "EUR", "to": "USD"})
        response = await service.create_currency(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()
        self.assertEqual(observed['currency'], 'USD')

if __name__ == '__main__':
    unittest.main()
