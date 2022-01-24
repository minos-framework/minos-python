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
    BrokerHandler,
    BrokerHandlerService,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)
from tests.utils import (
    BASE_PATH,
)


class TestBrokerHandlerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.publisher = InMemoryBrokerPublisher.from_config(self.config)
        self.subscriber_builder = InMemoryBrokerSubscriberBuilder

    def test_is_instance(self):
        service = BrokerHandlerService(config=self.config, publisher=self.publisher)
        self.assertIsInstance(service, Service)

    def test_handler(self):
        service = BrokerHandlerService(
            config=self.config, publisher=self.publisher, subscriber_builder=self.subscriber_builder
        )
        self.assertIsInstance(service.handler, BrokerHandler)

    async def test_start_stop(self):
        service = BrokerHandlerService(
            config=self.config, publisher=self.publisher, subscriber_builder=self.subscriber_builder
        )

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        run_mock = AsyncMock()

        service.handler.setup = setup_mock
        service.handler.destroy = destroy_mock
        service.handler.run = run_mock

        await service.start()

        self.assertEqual(1, setup_mock.call_count)
        self.assertEqual(1, run_mock.call_count)
        self.assertEqual(0, destroy_mock.call_count)

        setup_mock.reset_mock()
        destroy_mock.reset_mock()
        run_mock.reset_mock()

        await service.stop()

        self.assertEqual(0, setup_mock.call_count)
        self.assertEqual(0, run_mock.call_count)
        self.assertEqual(1, destroy_mock.call_count)


if __name__ == "__main__":
    unittest.main()
