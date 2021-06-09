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
    FakeModel,
)


class TestCommandRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.models = [FakeModel("foo"), FakeModel("bar")]
        self.command = Command("FooCreated", self.models, "12345678", "AddOrderReply")

    def test_command(self):
        request = CommandRequest(self.command)
        self.assertEqual(self.command, request.command)

    async def test_content(self):
        request = CommandRequest(self.command)
        self.assertEqual(self.models, await request.content())


class TestCommandResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.models = [FakeModel("foo"), FakeModel("bar")]

    async def test_content(self):
        response = CommandResponse(self.models)
        self.assertEqual(self.models, await response.content())

    async def test_content_single(self):
        response = CommandResponse(self.models[0])
        self.assertEqual([self.models[0]], await response.content())


if __name__ == "__main__":
    unittest.main()
