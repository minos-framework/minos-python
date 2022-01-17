import unittest
from unittest.mock import (
    AsyncMock,
)

from aiomisc import (
    Service,
)

from minos.common import (
    NotProvidedException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerConsumerService,
    BrokerHandler,
    BrokerHandlerService,
)
from minos.networks.brokers.publishers.queued.repositories.pg.publishers import (
    PostgreSqlBrokerPublisherRepositoryEnqueue,
)
from tests.utils import (
    BASE_PATH,
    FakeDispatcher,
)


class TestConsumerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.consumer = FakeDispatcher()

    def test_consumer_not_provider_raises(self):
        with self.assertRaises(NotProvidedException):
            BrokerConsumerService()

    def test_is_instance(self):
        # noinspection PyTypeChecker
        service = BrokerConsumerService(consumer=self.consumer)
        self.assertIsInstance(service, Service)

    async def test_start(self):
        # noinspection PyTypeChecker
        service = BrokerConsumerService(consumer=self.consumer)

        self.assertEqual(0, self.consumer.setup_count)
        self.assertEqual(0, self.consumer.setup_dispatch)
        self.assertEqual(0, self.consumer.setup_destroy)
        await service.start()
        self.assertEqual(1, self.consumer.setup_count)
        self.assertEqual(1, self.consumer.setup_dispatch)
        self.assertEqual(0, self.consumer.setup_destroy)
        await service.stop()
        self.assertEqual(1, self.consumer.setup_count)
        self.assertEqual(1, self.consumer.setup_dispatch)
        self.assertEqual(1, self.consumer.setup_destroy)


class TestBrokerHandlerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.publisher = PostgreSqlBrokerPublisherRepositoryEnqueue.from_config(self.config)

    def test_is_instance(self):
        service = BrokerHandlerService(config=self.config, publisher=self.publisher)
        self.assertIsInstance(service, Service)

    def test_handler(self):
        service = BrokerHandlerService(config=self.config, publisher=self.publisher)
        self.assertIsInstance(service.handler, BrokerHandler)

    async def test_start_stop(self):
        service = BrokerHandlerService(config=self.config, publisher=self.publisher)

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        dispatch_forever_mock = AsyncMock()

        service.handler.setup = setup_mock
        service.handler.destroy = destroy_mock
        service.handler.dispatch_forever = dispatch_forever_mock

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
