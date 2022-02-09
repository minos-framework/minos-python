import unittest
from unittest.mock import (
    call,
    patch,
)

from aiohttp import ClientResponseError
from aiomisc.circuit_breaker import CircuitBreakerStates

from minos.plugins.minos_discovery import (
    MinosDiscoveryClient,
)


class _Response:

    def __init__(self, ok):
        self.ok = ok

    def raise_for_status(self):
        """For testing purposes."""
        if not self.ok:
            # noinspection PyTypeChecker
            raise ClientResponseError(None, tuple())


class TestMinosDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = MinosDiscoveryClient("123.456.123.1", 1234, circuit_breaker_time=0.1)

    def test_route(self):
        # noinspection HttpUrlsUsage
        self.assertEqual("http://123.456.123.1:1234", self.client.route)

    @patch("aiohttp.ClientSession.post")
    async def test_subscribe(self, mock):
        mock.return_value.__aenter__.return_value = _Response(True)

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
        async def _fn_failure(*args, **kwargs):
            if self.client.circuit_breaker.state == CircuitBreakerStates.RECOVERING:
                return _Response(True)
            return _Response(False)

        mock.return_value.__aenter__ = _fn_failure

        await self.client.subscribe("56.56.56.56", 56, "test", [{"url": "/foo", "method": "POST"}])
        self.assertLess(0, mock.call_count)

    @patch("aiohttp.ClientSession.delete")
    async def test_unsubscribe(self, mock):
        mock.return_value.__aenter__.return_value = _Response(True)

        await self.client.unsubscribe("test")
        self.assertEqual(1, mock.call_count)
        # noinspection HttpUrlsUsage
        self.assertEqual(call("http://123.456.123.1:1234/microservices/test"), mock.call_args)

    @patch("aiohttp.ClientSession.delete")
    async def test_unsubscribe_raises(self, mock):
        async def _fn_failure(*args, **kwargs):
            if self.client.circuit_breaker.state == CircuitBreakerStates.RECOVERING:
                return _Response(True)
            return _Response(False)

        mock.return_value.__aenter__ = _fn_failure

        await self.client.unsubscribe("test")
        self.assertLess(0, mock.call_count)


if __name__ == "__main__":
    unittest.main()
