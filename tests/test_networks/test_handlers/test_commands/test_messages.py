"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Command,
)
from minos.networks import (
    CommandRequest,
    CommandResponse,
)
from tests.utils import (
    Foo,
)


class TestCommandRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.command = Command("FooCreated", [Foo("test"), Foo("tost")], "12345678", "AddOrderReply")

    def test_command(self):
        request = CommandRequest(self.command)
        self.assertEqual(self.command, request.command)

    async def test_content(self):
        request = CommandRequest(self.command)
        self.assertEqual([Foo("test"), Foo("tost")], await request.content())


class TestCommandResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.items = [Foo("test"), Foo("tost")]

    async def test_content(self):
        response = CommandResponse(self.items)
        self.assertEqual(self.items, await response.content())

    async def test_content_single(self):
        response = CommandResponse(self.items[0])
        self.assertEqual([self.items[0]], await response.content())


if __name__ == "__main__":
    unittest.main()
