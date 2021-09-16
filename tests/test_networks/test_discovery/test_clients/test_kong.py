
import unittest
from collections import (
    namedtuple,
)
from unittest.mock import (
    call,
    patch,
)

from minos.networks import (
    MinosDiscoveryConnectorException,
)
from minos.networks.discovery.clients.kong import (
    KongDiscovery,
)

_Response = namedtuple("Response", ["ok"])


async def _fn(*args, **kwargs):
    return _Response(True)


async def _fn_failure(*args, **kwargs):
    return _Response(False)


async def _fn_raises(*args, **kwargs):
    raise ValueError


class TestKong(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = KongDiscovery("123.456.123.1", 1234)

    def test_route(self):
        # noinspection HttpUrlsUsage
        self.assertEqual("http://123.456.123.1:1234", self.client.route)

    @patch("aiohttp.ClientSession.post")
    async def test_subscribe(self, mock):
        mock.return_value.__aenter__ = _fn

        await self.client.subscribe(
            "56.56.56.56", 56, "test", [{"url": "/foo", "method": "POST"}, {"url": "/bar", "method": "GET"}]
        )

        self.assertEqual(2, mock.call_count)

        # noinspection HttpUrlsUsage
        expected_service = call(
            "http://123.456.123.1:1234/services", json={"name": "test", "url": "http://56.56.56.56:56"},
        )
        self.assertEqual(expected_service, mock.call_args_list[0])

        # noinspection HttpUrlsUsage
        expected_paths = call("http://123.456.123.1:1234/test/routes", json={"paths": ["/foo", "/bar"]},)
        self.assertEqual(expected_paths, mock.call_args_list[1])

    @patch("aiohttp.ClientSession.post")
    async def test_subscribe_raises_failure(self, mock):
        mock.return_value.__aenter__ = _fn_failure

        with self.assertRaises(MinosDiscoveryConnectorException):
            await self.client.subscribe("56.56.56.56", 56, "test", [{"url": "/foo", "method": "POST"}], retry_delay=0)
        self.assertEqual(3, mock.call_count)

    @patch("aiohttp.ClientSession.post")
    async def test_subscribe_raises_exception(self, mock):
        mock.return_value.__aenter__ = _fn_raises

        with self.assertRaises(MinosDiscoveryConnectorException):
            await self.client.subscribe("56.56.56.56", 56, "test", [{"url": "/foo", "method": "POST"}], retry_delay=0)
        self.assertEqual(3, mock.call_count)

    @patch("aiohttp.ClientSession.delete")
    async def test_unsubscribe(self, mock):
        mock.return_value.__aenter__ = _fn
        await self.client.unsubscribe("test")
        self.assertEqual(1, mock.call_count)
        # noinspection HttpUrlsUsage
        self.assertEqual(call("http://123.456.123.1:1234/services/test"), mock.call_args)

    @patch("aiohttp.ClientSession.delete")
    async def test_unsubscribe_raises_failure(self, mock):
        mock.return_value.__aenter__ = _fn_failure

        with self.assertRaises(MinosDiscoveryConnectorException):
            await self.client.unsubscribe("test", retry_delay=0)
        self.assertEqual(3, mock.call_count)

    @patch("aiohttp.ClientSession.delete")
    async def test_unsubscribe_raises_exception(self, mock):
        mock.return_value.__aenter__ = _fn_raises

        with self.assertRaises(MinosDiscoveryConnectorException):
            await self.client.unsubscribe("test", retry_delay=0)
        self.assertEqual(3, mock.call_count)


if __name__ == "__main__":
    unittest.main()
