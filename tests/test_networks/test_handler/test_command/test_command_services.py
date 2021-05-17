import unittest
from unittest.mock import (
    MagicMock,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    MinosCommandPeriodicService,
    MinosCommandServerService,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosCommandServices(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_start(self):
        service = MinosCommandServerService(loop=None, config=self.config)

        async def _fn(consumer):
            self.assertEqual(service.consumer, consumer)

        mock = MagicMock(side_effect=_fn)
        service.dispatcher.handle_message = mock
        await service.start()
        self.assertTrue(1, mock.call_count)
        await service.stop()


class TestMinosQueueService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_start(self):
        service = MinosCommandPeriodicService(interval=1, loop=None, config=self.config)
        mock = MagicMock(side_effect=service.dispatcher.setup)
        service.dispatcher.setup = mock
        await service.start()
        self.assertTrue(1, mock.call_count)
        await service.stop()

    async def test_callback(self):
        service = MinosCommandPeriodicService(interval=1, loop=None, config=self.config)
        await service.dispatcher.setup()
        mock = MagicMock(side_effect=service.dispatcher.queue_checker)
        service.dispatcher.queue_checker = mock
        await service.callback()
        self.assertEqual(1, mock.call_count)
        await service.dispatcher.destroy()


if __name__ == "__main__":
    unittest.main()
