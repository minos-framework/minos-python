import unittest
from unittest.mock import (
    AsyncMock,
)

from aiomisc import (
    Service,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerConsumerService,
    BrokerHandler,
    BrokerHandlerService,
)
from tests.utils import (
    BASE_PATH,
    FakeDispatcher,
)


class TestConsumerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.dispatcher = FakeDispatcher()

    def test_is_instance(self):
        service = BrokerConsumerService(config=self.config, dispatcher=self.dispatcher)
        self.assertIsInstance(service, Service)

    async def test_start(self):
        # noinspection PyTypeChecker
        service = BrokerConsumerService(config=self.config, dispatcher=self.dispatcher)

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


class TestBrokerHandlerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_is_instance(self):
        service = BrokerHandlerService(config=self.config)
        self.assertIsInstance(service, Service)

    def test_dispatcher(self):
        service = BrokerHandlerService(config=self.config)
        self.assertIsInstance(service.dispatcher, BrokerHandler)

    async def test_start_stop(self):
        service = BrokerHandlerService(config=self.config)

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
