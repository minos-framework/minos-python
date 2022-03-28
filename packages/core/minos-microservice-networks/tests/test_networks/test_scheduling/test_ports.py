import unittest
import warnings
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
    PeriodicTaskScheduler,
    PeriodicPort,
    PeriodicTaskSchedulerService,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestPeriodicPort(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH

    def test_is_instance(self):
        service = PeriodicPort(config=self.config)
        self.assertIsInstance(service, Port)

    def test_dispatcher(self):
        service = PeriodicPort(config=self.config)
        self.assertIsInstance(service.scheduler, PeriodicTaskScheduler)

    async def test_start_stop(self):
        service = PeriodicPort(config=self.config)

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        start_mock = AsyncMock()
        stop_mock = AsyncMock()

        service.scheduler.setup = setup_mock
        service.scheduler.destroy = destroy_mock
        service.scheduler.start = start_mock
        service.scheduler.stop = stop_mock

        await service.start()

        self.assertEqual(1, setup_mock.call_count)
        self.assertEqual(1, start_mock.call_count)
        self.assertEqual(0, stop_mock.call_count)
        self.assertEqual(0, destroy_mock.call_count)

        setup_mock.reset_mock()
        destroy_mock.reset_mock()
        start_mock.reset_mock()
        stop_mock.reset_mock()

        await service.stop()

        self.assertEqual(0, setup_mock.call_count)
        self.assertEqual(0, start_mock.call_count)
        self.assertEqual(1, stop_mock.call_count)
        self.assertEqual(1, destroy_mock.call_count)


class TestPeriodicTaskSchedulerService(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(PeriodicTaskSchedulerService, PeriodicPort))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            port = PeriodicTaskSchedulerService(config=CONFIG_FILE_PATH)
            self.assertIsInstance(port, PeriodicPort)


if __name__ == "__main__":
    unittest.main()
