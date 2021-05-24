import unittest
from unittest.mock import (
    MagicMock,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandConsumerService,
    CommandHandlerService,
)
from tests.utils import (
    BASE_PATH,
)


class TestCommandConsumerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_start(self):
        service = CommandConsumerService(loop=None, config=self.config)

        async def _fn(consumer):
            self.assertEqual(service.consumer, consumer)

        mock = MagicMock(side_effect=_fn)
        service.dispatcher.handle_message = mock
        await service.start()
        self.assertTrue(1, mock.call_count)
        await service.stop()


class TestCommandHandlerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_start(self):
        service = CommandHandlerService(interval=1, loop=None, config=self.config)
        mock = MagicMock(side_effect=service.dispatcher.setup)
        service.dispatcher.setup = mock
        await service.start()
        self.assertTrue(1, mock.call_count)
        await service.stop()

    async def test_callback(self):
        service = CommandHandlerService(interval=1, loop=None, config=self.config)
        await service.dispatcher.setup()
        mock = MagicMock(side_effect=service.dispatcher.dispatch)
        service.dispatcher.dispatch = mock
        await service.callback()
        self.assertEqual(1, mock.call_count)
        await service.dispatcher.destroy()


if __name__ == "__main__":
    unittest.main()
