"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from json import (
    JSONDecodeError,
)
from unittest.mock import (
    PropertyMock,
    patch,
)

from minos.networks import (
    HttpRequest,
    HttpResponse,
)
from tests.utils import (
    FakeModel,
)


class MockedRequest:
    def __init__(self, data=None):
        self.data = data
        self.remote = "test"

    def __repr__(self):
        return "repr"

    async def json(self):
        if self.data is None:
            raise JSONDecodeError("", "", 1)
        return self.data


class TestHttpRequest(unittest.IsolatedAsyncioTestCase):
    def test_raw_request(self):
        raw_request = MockedRequest()
        request = HttpRequest(raw_request)
        self.assertEqual(raw_request, request.raw_request)

    def test_repr(self):
        request = HttpRequest(MockedRequest())
        self.assertEqual("HttpRequest(repr)", repr(request))

    def test_eq_true(self):
        request = MockedRequest()
        self.assertEqual(HttpRequest(request), HttpRequest(request))

    def test_eq_false(self):
        self.assertNotEqual(HttpRequest(MockedRequest()), HttpRequest(MockedRequest()))

    async def test_content(self):
        raw_request = MockedRequest(
            [
                {"id": 1, "version": 1, "doors": 3, "color": "blue", "owner": None},
                {"id": 2, "version": 1, "doors": 5, "color": "red", "owner": None},
            ]
        )
        request = HttpRequest(raw_request)
        self.assertEqual(
            raw_request.data, await request.content(),
        )

    async def test_content_single(self):
        raw_request = MockedRequest({"id": 1, "version": 1, "doors": 3, "color": "blue", "owner": None})
        request = HttpRequest(raw_request)
        self.assertEqual(
            [raw_request.data], await request.content(),
        )

    async def test_content_no_json(self):
        raw_request = MockedRequest()
        with patch("minos.networks.HttpRequest._raw_url_args", new_callable=PropertyMock) as mock:
            mock.return_value = {"id": 1}
            request = HttpRequest(raw_request)
            self.assertEqual(
                [1], await request.content(),
            )


class TestHttpResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.models = [FakeModel("foo"), FakeModel("bar")]

    async def test_content(self):
        response = HttpResponse(self.models)
        self.assertEqual([item.avro_data for item in self.models], await response.raw_content())

    async def test_content_single(self):
        response = HttpResponse(self.models[0])
        self.assertEqual([self.models[0].avro_data], await response.raw_content())


if __name__ == "__main__":
    unittest.main()
