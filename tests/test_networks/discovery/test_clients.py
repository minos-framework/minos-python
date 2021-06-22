"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    call,
    patch,
)

from minos.networks import (
    MinosDiscoveryClient,
)


async def _fn(*args, **kwargs):
    pass


class TestMinosDiscoveryClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = MinosDiscoveryClient("123.456.123.1", 1234)

    def test_route(self):
        # noinspection HttpUrlsUsage
        self.assertEqual("http://123.456.123.1:1234", self.client.route)

    @patch("aiohttp.ClientSession.post")
    async def test_subscribe(self, mock):
        mock.side_effect = _fn

        await self.client.subscribe("56.56.56.56", 56, "test")

        self.assertEqual(1, mock.call_count)
        # noinspection HttpUrlsUsage
        expected = call("http://123.456.123.1:1234/subscribe", json={"ip": "56.56.56.56", "port": 56, "name": "test"})
        self.assertEqual(expected, mock.call_args)

    @patch("aiohttp.ClientSession.post")
    async def test_unsubscribe(self, mock):
        mock.side_effect = _fn
        await self.client.unsubscribe("test")
        self.assertEqual(1, mock.call_count)
        # noinspection HttpUrlsUsage
        self.assertEqual(call("http://123.456.123.1:1234/unsubscribe?name=test"), mock.call_args)


if __name__ == "__main__":
    unittest.main()
