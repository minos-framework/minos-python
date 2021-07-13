"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest

from dependency_injector import (
    containers,
    providers,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.cqrs import (
    Service,
)
from tests.utils import (
    BASE_PATH,
    FakeSagaManager,
)


class TestService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.saga_manager = FakeSagaManager()

        self.container = containers.DynamicContainer()
        self.container.config = providers.Object(self.config)
        self.container.saga_manager = providers.Object(self.saga_manager)

        await self.container.saga_manager().setup()
        self.container.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self):
        self.container.unwire()
        await self.container.saga_manager().destroy()
        await super().asyncTearDown()

    async def test_constructor(self):
        service = Service()
        self.assertEqual(self.config, service.config)
        self.assertEqual(self.saga_manager, service.saga_manager)


if __name__ == "__main__":
    unittest.main()
