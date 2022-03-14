import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
    patch,
)

from minos.common import (
    MinosSetup,
)
from minos.networks import (
    HttpAdapter,
    HttpConnector,
    HttpResponse,
    HttpResponseException,
    InMemoryRequest,
    Request,
    Response,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    FakeHttpConnector,
    FakeService,
)


class _Cls:
    @staticmethod
    async def _fn(request: Request) -> Response:
        return HttpResponse(await request.content())

    @staticmethod
    async def _fn_raises_response(request: Request) -> Response:
        raise HttpResponseException("")

    @staticmethod
    async def _fn_raises_exception(request: Request) -> Response:
        raise ValueError


class TestHttpConnector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.adapter = HttpAdapter.from_config(CONFIG_FILE_PATH)
        self.connector = FakeHttpConnector.from_config(CONFIG_FILE_PATH)

        self.request_mock = AsyncMock(side_effect=lambda x: InMemoryRequest(x))
        self.response_mock = AsyncMock(return_value="bar")
        self.error_response_mock = AsyncMock(return_value="foobar")

        self.connector._build_request = self.request_mock
        self.connector._build_response = self.response_mock
        self.connector._build_error_response = self.error_response_mock

    def test_abstract(self):
        self.assertTrue(issubclass(HttpConnector, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"_mount_route", "_build_error_response", "_build_request", "_build_response", "_start", "_stop"},
            HttpConnector.__abstractmethods__,
        )

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

        self.connector.adapt_callback = adapt_mock
        self.connector._mount_route = mount_mock

        self.connector.mount_route("/path/to/callback", "POST", FakeService.create_ticket)

        self.assertEqual([call(FakeService.create_ticket)], adapt_mock.call_args_list)
        self.assertEqual([call("/path/to/callback", "POST", _fn)], mount_mock.call_args_list)

    def test_mount_routes(self):
        mount_mock = MagicMock()
        self.connector.mount_route = mount_mock

        self.connector.mount_routes()

        self.assertEqual(
            [call(decorator.path, decorator.method, callback) for decorator, callback in self.connector.routes.items()],
            mount_mock.call_args_list,
        )

    async def test_get_callback(self):
        handler = self.connector.adapt_callback(_Cls._fn)
        response = await handler("foo")
        self.assertEqual("bar", response)

        self.assertEqual([call("foo")], self.request_mock.call_args_list)
        self.assertEqual([call(HttpResponse("foo"))], self.response_mock.call_args_list)
        self.assertEqual([], self.error_response_mock.call_args_list)

    async def test_get_callback_raises_response(self):
        handler = self.connector.adapt_callback(_Cls._fn_raises_response)

        with patch("traceback.format_exc", return_value="error"):
            response = await handler("foo")

        self.assertEqual("foobar", response)

        self.assertEqual([call("foo")], self.request_mock.call_args_list)
        self.assertEqual([], self.response_mock.call_args_list)
        self.assertEqual([call("error", 400)], self.error_response_mock.call_args_list)

    async def test_get_callback_raises_exception(self):
        handler = self.connector.adapt_callback(_Cls._fn_raises_exception)

        with patch("traceback.format_exc", return_value="error"):
            response = await handler("foo")
        self.assertEqual("foobar", response)

        self.assertEqual([call("foo")], self.request_mock.call_args_list)
        self.assertEqual([], self.response_mock.call_args_list)
        self.assertEqual([call("error", 500)], self.error_response_mock.call_args_list)

    async def test_start_stop(self):
        star_mock = AsyncMock()
        stop_mock = AsyncMock()
        mount_mock = MagicMock()

        self.connector._start = star_mock
        self.connector._stop = stop_mock
        self.connector.mount_routes = mount_mock

        await self.connector.start()

        self.assertEqual([call()], mount_mock.call_args_list)
        self.assertEqual([call()], star_mock.call_args_list)
        self.assertEqual([], stop_mock.call_args_list)

        mount_mock.reset_mock()
        star_mock.reset_mock()
        stop_mock.reset_mock()

        await self.connector.stop()

        self.assertEqual([], mount_mock.call_args_list)
        self.assertEqual([], star_mock.call_args_list)
        self.assertEqual([call()], stop_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
