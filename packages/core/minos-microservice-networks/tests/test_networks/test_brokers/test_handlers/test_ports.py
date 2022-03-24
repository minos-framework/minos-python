import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    Port,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerHandler,
    BrokerHandlerPort,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestBrokerHandlerPort(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def setUp(self) -> None:
        super().setUp()
        self.publisher = InMemoryBrokerPublisher.from_config(self.config)
        self.subscriber_builder = InMemoryBrokerSubscriberBuilder()

    def test_is_instance(self):
        service = BrokerHandlerPort(config=self.config, publisher=self.publisher)
        self.assertIsInstance(service, Port)

    def test_handler(self):
        service = BrokerHandlerPort(
            config=self.config, publisher=self.publisher, subscriber_builder=self.subscriber_builder
        )
        self.assertIsInstance(service.handler, BrokerHandler)

    async def test_start_stop(self):
        service = BrokerHandlerPort(
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
