import sys
import unittest

from src import (
    NotifierQueryService,
)

from tests.utils import (
    build_dependency_injector,
)


class TestNotifierQueryService(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = NotifierQueryService()
        self.assertIsInstance(service, NotifierQueryService)


if __name__ == '__main__':
    unittest.main()