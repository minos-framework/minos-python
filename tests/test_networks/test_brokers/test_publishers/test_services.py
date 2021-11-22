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
    BrokerConsumer,
    BrokerProducer,
    BrokerProducerService,
)
from tests.utils import (
    BASE_PATH,
)


class TestProducerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.consumer = BrokerConsumer.from_config(self.config)

    def test_is_instance(self):
        service = BrokerProducerService(config=self.config, consumer=self.consumer)
        self.assertIsInstance(service, Service)

    def test_dispatcher(self):
        service = BrokerProducerService(config=self.config, consumer=self.consumer)
        self.assertIsInstance(service.dispatcher, BrokerProducer)

    async def test_start_stop(self):
        service = BrokerProducerService(config=self.config, consumer=self.consumer)

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
