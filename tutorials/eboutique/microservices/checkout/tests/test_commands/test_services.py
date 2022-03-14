import sys
import unittest

from src import (
    Checkout,
    CheckoutCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestCheckoutCommandService(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = CheckoutCommandService()
        self.assertIsInstance(service, CheckoutCommandService)

    async def test_create_checkout(self):
        service = CheckoutCommandService()

        request = InMemoryRequest({})
        response = await service.create_checkout(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()
        expected = Checkout(
            created_at=observed.created_at,
            updated_at=observed.updated_at,
            uuid=observed.uuid,
            version=observed.version,
        )

        self.assertEqual(expected, observed)


if __name__ == '__main__':
    unittest.main()