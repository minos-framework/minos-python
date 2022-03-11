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
    FakeHttpConnector,
)


class TestHttpPort(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.connector = FakeHttpConnector.from_config(CONFIG_FILE_PATH)

    def test_is_instance(self):
        service = HttpPort(connector=self.connector)
        self.assertIsInstance(service, Port)

    def test_connector(self):
        service = HttpPort(connector=self.connector)
        self.assertEqual(service.connector, self.connector)

    def test_http_connector(self):
        service = HttpPort(http_connector=self.connector)
        self.assertEqual(service.connector, self.connector)

    def test_missing_connector(self):
        service = HttpPort()
        with self.assertRaises(NotProvidedException):
            service.connector

    async def test_start_stop(self):
        service = HttpPort(connector=self.connector)

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        start_mock = AsyncMock()
        stop_mock = AsyncMock()

        service.connector.setup = setup_mock
        service.connector.destroy = destroy_mock

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
