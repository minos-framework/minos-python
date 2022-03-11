import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    NotProvidedException,
)
from minos.networks import (
    HttpPort,
    Port,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    FakeHttpApplication,
)


class TestHttpPort(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.application = FakeHttpApplication.from_config(CONFIG_FILE_PATH)

    def test_is_instance(self):
        service = HttpPort(application=self.application)
        self.assertIsInstance(service, Port)

    def test_application(self):
        service = HttpPort(application=self.application)
        self.assertEqual(service.application, self.application)

    def test_http_application(self):
        service = HttpPort(http_application=self.application)
        self.assertEqual(service.application, self.application)

    def test_missing_application(self):
        service = HttpPort()
        with self.assertRaises(NotProvidedException):
            service.application

    async def test_start_stop(self):
        service = HttpPort(application=self.application)

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        start_mock = AsyncMock()
        stop_mock = AsyncMock()

        service.application.setup = setup_mock
        service.application.destroy = destroy_mock

        await service.start()

        self.assertEqual(1, setup_mock.call_count)
        self.assertEqual(0, destroy_mock.call_count)

        setup_mock.reset_mock()
        destroy_mock.reset_mock()
        start_mock.reset_mock()
        stop_mock.reset_mock()

        await service.stop()

        self.assertEqual(0, setup_mock.call_count)
        self.assertEqual(1, destroy_mock.call_count)


if __name__ == "__main__":
    unittest.main()
