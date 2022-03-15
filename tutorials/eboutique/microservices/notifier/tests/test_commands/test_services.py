import sys
import unittest

from src import (
    Notifier,
    NotifierCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestNotifierCommandService(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = NotifierCommandService()
        self.assertIsInstance(service, NotifierCommandService)

    async def test_create_notifier(self):
        service = NotifierCommandService()

        request = InMemoryRequest({})
        response = await service.create_notifier(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()
        expected = Notifier(
            created_at=observed.created_at,
            updated_at=observed.updated_at,
            uuid=observed.uuid,
            version=observed.version,
        )

        self.assertEqual(expected, observed)


if __name__ == '__main__':
    unittest.main()