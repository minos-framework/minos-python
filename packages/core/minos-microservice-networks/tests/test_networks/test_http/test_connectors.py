import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    HttpAdapter,
    HttpConnector,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    FakeHttpConnector,
    FakeService,
)


class TestHttpConnector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.adapter = HttpAdapter.from_config(CONFIG_FILE_PATH)
        self.connector = FakeHttpConnector.from_config(CONFIG_FILE_PATH)

    def test_abstract(self):
        self.assertTrue(issubclass(HttpConnector, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_mount_route", "_adapt_callback", "_start", "_stop"}, HttpConnector.__abstractmethods__)

    def test_host(self):
        self.assertEqual("localhost", self.connector.host)

    def test_port(self):
        self.assertEqual(8080, self.connector.port)

    def test_adapter(self):
        self.assertEqual(self.adapter, self.connector.adapter)

    def test_routes(self):
        self.assertEqual(self.adapter.routes.keys(), self.connector.routes.keys())

    def test_mount_route(self):
        def _fn():
            pass

        adapt_mock = MagicMock(return_value=_fn)
        mount_mock = MagicMock()

        self.connector._adapt_callback = adapt_mock
        self.connector._mount_route = mount_mock

        self.connector.mount_route("/path/to/callback", "POST", FakeService.create_ticket)

        self.assertEqual([call(FakeService.create_ticket)], adapt_mock.call_args_list)
        self.assertEqual([call("/path/to/callback", "POST", _fn)], mount_mock.call_args_list)

    async def test_setup_destroy(self):
        star_mock = AsyncMock()
        stop_mock = AsyncMock()
        mount_mock = MagicMock()

        self.connector._start = star_mock
        self.connector._stop = stop_mock
        self.connector.mount_route = mount_mock

        await self.connector.setup()

        self.assertEqual(
            [call(d.path, d.method, c) for d, c in self.connector.routes.items()], mount_mock.call_args_list
        )
        self.assertEqual([call()], star_mock.call_args_list)
        self.assertEqual([], stop_mock.call_args_list)

        mount_mock.reset_mock()
        star_mock.reset_mock()
        stop_mock.reset_mock()

        await self.connector.destroy()

        self.assertEqual([], mount_mock.call_args_list)
        self.assertEqual([], star_mock.call_args_list)
        self.assertEqual([call()], stop_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
