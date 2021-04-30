import unittest
from unittest.mock import (
    MagicMock,
)
from minos.common.configuration.config import (
    MinosConfig,
)
from minos.networks import (
    MinosEventServerService,
    MinosEventPeriodicService
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
)


"""
@pytest.fixture()
def services(config):
    return [
        MinosEventServerService(config=config),
        MinosEventPeriodicService(interval=0.5, delay=0, config=config),
    ]
"""

class TestMinosQueueService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_start(self):
        with self.config:
            service = MinosEventPeriodicService(interval=1, loop=None)
            service.dispatcher.setup = MagicMock(side_effect=service.dispatcher.setup)
            await service.start()
            self.assertTrue(1, service.dispatcher.setup.call_count)

    async def test_callback(self):
        with self.config:
            service = MinosEventPeriodicService(interval=1, loop=None)
            service.dispatcher.event_queue_checker = MagicMock(side_effect=service.dispatcher.event_queue_checker)
            await service.start()
            self.assertEqual(1, service.dispatcher.event_queue_checker.call_count)
