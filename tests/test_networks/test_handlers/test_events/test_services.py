"""tests.test_networks.test_handlers.test_events.test_services module."""

import unittest
from unittest.mock import (
    AsyncMock,
    patch,
)

from aiomisc import (
    Service,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandHandlerService,
    EventConsumerService,
    EventHandler,
    EventHandlerService,
)
from tests.utils import (
    BASE_PATH,
    FakeDispatcher,
)


class TestEventConsumerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    @patch("minos.networks.EventConsumer.from_config")
    async def test_start(self, mock):
        instance = FakeDispatcher()
        mock.return_value = instance

        service = EventConsumerService(loop=None, config=self.config)

        self.assertEqual(0, instance.setup_count)
        self.assertEqual(0, instance.setup_dispatch)
        self.assertEqual(0, instance.setup_destroy)
        await service.start()
        self.assertEqual(1, instance.setup_count)
        self.assertEqual(1, instance.setup_dispatch)
        self.assertEqual(0, instance.setup_destroy)
        await service.stop()
        self.assertEqual(1, instance.setup_count)
        self.assertEqual(1, instance.setup_dispatch)
        self.assertEqual(1, instance.setup_destroy)


class TestEventHandlerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_is_instance(self):
        service = EventHandlerService(config=self.config)
        self.assertIsInstance(service, Service)

    def test_dispatcher(self):
        service = CommandHandlerService(config=self.config)
        self.assertIsInstance(service.dispatcher, EventHandler)

    async def test_start_stop(self):
        service = EventHandlerService(config=self.config)

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        dispatch_forever_mock = AsyncMock()

        service.dispatcher.setup = setup_mock
        service.dispatcher.destroy = destroy_mock
        service.dispatcher.dispatch_forever = dispatch_forever_mock

        await service.start()

        self.assertEqual(1, setup_mock.call_count)
        self.assertEqual(1, dispatch_forever_mock.call_count)
        self.assertEqual(0, destroy_mock.call_count)

        setup_mock.reset_mock()
        destroy_mock.reset_mock()
        dispatch_forever_mock.reset_mock()

        await service.stop()

        self.assertEqual(0, setup_mock.call_count)
        self.assertEqual(0, dispatch_forever_mock.call_count)
        self.assertEqual(1, destroy_mock.call_count)


if __name__ == "__main__":
    unittest.main()
