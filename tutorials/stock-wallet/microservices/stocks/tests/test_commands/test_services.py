import sys
import unittest

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

    async def test_create_stocks(self):
        service = StocksCommandService()

        request = InMemoryRequest({})
        response = await service.create_stocks(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()
        expected = Stocks(
            created_at=observed.created_at,
            updated_at=observed.updated_at,
            uuid=observed.uuid,
            version=observed.version,
        )

        self.assertEqual(expected, observed)


if __name__ == '__main__':
    unittest.main()