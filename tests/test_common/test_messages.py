"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from abc import (
    ABC,
)

from minos.common import (
    Request,
    Response,
)
from tests.model_classes import (
    Foo,
)


class TestRequest(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(Request, ABC))
        self.assertEqual({"content", "__eq__", "__repr__"}, Request.__abstractmethods__)


class TestResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data = [Foo("blue"), Foo("red")]

    async def test_content(self):
        response = Response(self.data)
        self.assertEqual(self.data, await response.content())

    async def test_content_single(self):
        response = Response(self.data[0])
        self.assertEqual(self.data[0], await response.content())

    async def test_content_simple(self):
        response = Response(1234)
        self.assertEqual(1234, await response.content())

    async def test_raw_content(self):
        response = Response(self.data)
        self.assertEqual([item.avro_data for item in self.data], await response.raw_content())

    async def test_raw_content_single(self):
        response = Response(self.data[0])
        self.assertEqual(self.data[0].avro_data, await response.raw_content())

    async def test_raw_content_simple(self):
        response = Response(1234)
        self.assertEqual(1234, await response.raw_content())

    async def test_eq_true(self):
        self.assertEqual(Response(self.data), Response(self.data))

    async def test_eq_false(self):
        self.assertNotEqual(Response(self.data[0]), Response(self.data[1]))

    async def test_repr(self):
        response = Response(self.data)
        self.assertEqual("Response([Foo(text=blue), Foo(text=red)])", repr(response))


if __name__ == "__main__":
    unittest.main()
