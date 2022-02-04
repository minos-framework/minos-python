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
from minos.plugins.minos_discovery import (
    MinosDiscoveryClient,
)

_Response = namedtuple("Response", ["ok"])


async def _fn(*args, **kwargs):
    return _Response(True)


async def _fn_failure(*args, **kwargs):
    return _Response(False)


async def _fn_raises(*args, **kwargs):
    raise ValueError


class TestMinosDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = MinosDiscoveryClient("123.456.123.1", 1234)

    def test_route(self):
        # noinspection HttpUrlsUsage
        self.assertEqual("http://123.456.123.1:1234", self.client.route)

    @patch("aiohttp.ClientSession.post")
    async def test_subscribe(self, mock):
        mock.return_value.__aenter__ = _fn

        await self.client.subscribe(
            "56.56.56.56", 56, "test", [{"url": "/foo", "method": "POST"}, {"url": "/bar", "method": "GET"}]
        )

        self.assertEqual(1, mock.call_count)
        # noinspection HttpUrlsUsage
        expected = call(
            "http://123.456.123.1:1234/microservices/test",
            json={"address": "56.56.56.56", "port": 56, "endpoints": [["POST", "/foo"], ["GET", "/bar"]]},
        )
        self.assertEqual(expected, mock.call_args)

    @patch("aiohttp.ClientSession.post")
    async def test_subscribe_raises(self, mock):
        mock.return_value.__aenter__ = _fn_failure

        with self.assertRaises(MinosDiscoveryConnectorException):
            await self.client.subscribe("56.56.56.56", 56, "test", [{"url": "/foo", "method": "POST"}], retry_delay=0)
        self.assertEqual(3, mock.call_count)

    @patch("aiohttp.ClientSession.delete")
    async def test_unsubscribe(self, mock):
        mock.return_value.__aenter__ = _fn
        await self.client.unsubscribe("test")
        self.assertEqual(1, mock.call_count)
        # noinspection HttpUrlsUsage
        self.assertEqual(call("http://123.456.123.1:1234/microservices/test"), mock.call_args)

    @patch("aiohttp.ClientSession.delete")
    async def test_unsubscribe_raises(self, mock):
        mock.return_value.__aenter__ = _fn_failure

        with self.assertRaises(MinosDiscoveryConnectorException):
            await self.client.unsubscribe("test", retry_delay=0)
        self.assertEqual(3, mock.call_count)


if __name__ == "__main__":
    unittest.main()
