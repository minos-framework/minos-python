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
    BrokerPublisherService,
    PostgreSqlQueuedKafkaBrokerPublisher,
)
from tests.utils import (
    BASE_PATH,
)


class TestBrokerPublisherService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.publisher = PostgreSqlQueuedKafkaBrokerPublisher.from_config(self.config)

    def test_not_provided_raises(self):
        with self.assertRaises(NotProvidedException):
            BrokerPublisherService()

    def test_is_instance(self):
        service = BrokerPublisherService(self.publisher)
        self.assertIsInstance(service, Service)

    def test_dispatcher(self):
        service = BrokerPublisherService(self.publisher)
        self.assertEqual(self.publisher, service.impl)

    async def test_start_stop(self):
        service = BrokerPublisherService(self.publisher)

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        run_mock = AsyncMock()

        service.impl.setup = setup_mock
        service.impl.destroy = destroy_mock
        service.impl.run = run_mock

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
