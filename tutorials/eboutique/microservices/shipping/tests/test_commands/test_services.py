import sys
import unittest

from src import (
    ShippingCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestShippingCommandService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = ShippingCommandService()
        self.assertIsInstance(service, ShippingCommandService)

    async def test_create_shipping_quote(self):
        service = ShippingCommandService()

        request = InMemoryRequest({"destination": "Paris", "items": 3})
        response = await service.create_shipping(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()
        self.assertEqual(1052, observed["distance"])
        self.assertEqual(11, observed["quote"])


if __name__ == "__main__":
    unittest.main()
