"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    Command,
)
from minos.networks import (
    CommandRequest,
    CommandResponse,
)
from tests.utils import (
    FakeModel,
)


class TestCommandRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data = [FakeModel("foo"), FakeModel("bar")]
        self.saga = uuid4()
        self.command = Command("FooCreated", self.data, self.saga, "AddOrderReply")

    def test_repr(self):
        request = CommandRequest(self.command)
        expected = (
            "CommandRequest(Command(topic=FooCreated, data=[FakeModel(text=foo), FakeModel(text=bar)], "
            f"saga={self.saga!s}, reply_topic=AddOrderReply))"
        )
        self.assertEqual(expected, repr(request))

    def test_eq_true(self):
        self.assertEqual(CommandRequest(self.command), CommandRequest(self.command))

    def test_eq_false(self):
        another = CommandRequest(Command("FooUpdated", self.data, self.saga, "AddOrderReply"))
        self.assertNotEqual(CommandRequest(self.command), another)

    def test_command(self):
        request = CommandRequest(self.command)
        self.assertEqual(self.command, request.command)

    async def test_content(self):
        request = CommandRequest(self.command)
        self.assertEqual(self.data, await request.content())

    async def test_content_single(self):
        request = CommandRequest(Command("FooCreated", self.data[0], self.saga, "AddOrderReply"))
        self.assertEqual(self.data[0], await request.content())

    async def test_content_simple(self):
        request = CommandRequest(Command("FooCreated", 1234, self.saga, "AddOrderReply"))
        self.assertEqual(1234, await request.content())


class TestCommandResponse(unittest.IsolatedAsyncioTestCase):
    async def test_content(self):
        response = CommandResponse([FakeModel("foo"), FakeModel("bar")])
        self.assertEqual([FakeModel("foo"), FakeModel("bar")], await response.content())

    async def test_content_single(self):
        response = CommandResponse(FakeModel("foo"))
        self.assertEqual(FakeModel("foo"), await response.content())


if __name__ == "__main__":
    unittest.main()
