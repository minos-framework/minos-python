"""tests.test_networks.test_handlers.test_dynamic.test_services module."""

import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    DynamicConsumerService,
)
from tests.utils import (
    BASE_PATH,
    FakeDispatcher,
)


class TestDynamicConsumerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.dispatcher = FakeDispatcher()

    async def test_start(self):
        # noinspection PyTypeChecker
        service = DynamicConsumerService(config=self.config, dispatcher=self.dispatcher)

        self.assertEqual(0, self.dispatcher.setup_count)
        self.assertEqual(0, self.dispatcher.setup_dispatch)
        self.assertEqual(0, self.dispatcher.setup_destroy)
        await service.start()
        self.assertEqual(1, self.dispatcher.setup_count)
        self.assertEqual(1, self.dispatcher.setup_dispatch)
        self.assertEqual(0, self.dispatcher.setup_destroy)
        await service.stop()
        self.assertEqual(1, self.dispatcher.setup_count)
        self.assertEqual(1, self.dispatcher.setup_dispatch)
        self.assertEqual(1, self.dispatcher.setup_destroy)


if __name__ == "__main__":
    unittest.main()
