import unittest
from unittest.mock import (
    MagicMock, patch,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    EventConsumerService,
    EventHandlerService,
)
from tests.utils import (
    BASE_PATH, FakeDispatcher,
)


class TestMinosEventServices(PostgresAsyncTestCase):
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


class TestMinosQueueService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_start(self):
        service = EventHandlerService(interval=1, loop=None, config=self.config)
        mock = MagicMock(side_effect=service.dispatcher.setup)
        service.dispatcher.setup = mock
        await service.start()
        self.assertTrue(1, mock.call_count)
        await service.stop()

    async def test_callback(self):
        service = EventHandlerService(interval=1, loop=None, config=self.config)
        await service.dispatcher.setup()
        mock = MagicMock(side_effect=service.dispatcher.dispatch)
        service.dispatcher.dispatch = mock
        await service.callback()
        self.assertEqual(1, mock.call_count)
        await service.dispatcher.destroy()


if __name__ == "__main__":
    unittest.main()
