"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.cqrs import (
    Service,
)
from tests.utils import (
    BASE_PATH,
    FakeCommandService,
    FakeQueryService,
    FakeSagaManager,
    FakeService,
)


class TestServices(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = FakeSagaManager()
        self.service = FakeService(config=self.config, saga_manager=self.saga_manager)

    async def test_constructor(self):
        self.assertEqual(self.config, self.service.config)
        self.assertEqual(self.saga_manager, self.service.saga_manager)


class TestQueryService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = FakeSagaManager()
        self.service = FakeQueryService(config=self.config, saga_manager=self.saga_manager)

    async def test_query_service_constructor(self):
        self.assertIsInstance(self.service, Service)


class TestCommandService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.saga_manager = FakeSagaManager()
        self.service = FakeCommandService(config=self.config, saga_manager=self.saga_manager)

    async def test_query_service_constructor(self):
        self.assertIsInstance(self.service, Service)


if __name__ == "__main__":
    unittest.main()
