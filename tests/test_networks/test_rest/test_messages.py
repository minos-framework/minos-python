"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
import uuid
from json import (
    JSONDecodeError,
)
from unittest.mock import (
    PropertyMock,
    patch,
)

from yarl import (
    URL,
)

from minos.common import (
    ModelType,
)
from minos.networks import (
    RestRequest,
    RestResponse,
)
from tests.utils import (
    FakeModel,
)


class MockedRequest:
    def __init__(self, data=None):
        self.data = data
        self.remote = "127.0.0.1"
        self.rel_url = URL("localhost")
        self.match_info = dict()
        self.headers = {"User": str(uuid.uuid1())}

    def __repr__(self):
        return "repr"

    async def json(self):
        if self.data is None:
            raise JSONDecodeError("", "", 1)
        return self.data


class TestRestRequest(unittest.IsolatedAsyncioTestCase):
    def test_raw_request(self):
        raw_request = MockedRequest()
        request = RestRequest(raw_request)
        self.assertEqual(raw_request, request.raw_request)

    def test_repr(self):
        request = RestRequest(MockedRequest())
        self.assertEqual("RestRequest(repr)", repr(request))

    def test_eq_true(self):
        request = MockedRequest()
        self.assertEqual(RestRequest(request), RestRequest(request))

    def test_eq_false(self):
        self.assertNotEqual(RestRequest(MockedRequest()), RestRequest(MockedRequest()))

    def test_user(self):
        raw_request = MockedRequest()
        request = RestRequest(raw_request)
        user = request.user
        self.assertEqual(uuid.UUID(raw_request.headers["User"]), user)

    async def test_content(self):
        Content = ModelType.build(
            "Content", {"id": int, "version": int, "doors": int, "color": str, "owner": type(None)}
        )
        expected = [
            Content(id=1, version=1, doors=3, color="blue", owner=None),
            Content(id=2, version=1, doors=5, color="red", owner=None),
        ]

        raw_request = MockedRequest(
            [
                {"id": 1, "version": 1, "doors": 3, "color": "blue", "owner": None},
                {"id": 2, "version": 1, "doors": 5, "color": "red", "owner": None},
            ]
        )
        request = RestRequest(raw_request)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_single(self):
        Content = ModelType.build(
            "Content", {"id": int, "version": int, "doors": int, "color": str, "owner": type(None)}
        )
        expected = Content(id=1, version=1, doors=3, color="blue", owner=None)

        raw_request = MockedRequest({"id": 1, "version": 1, "doors": 3, "color": "blue", "owner": None})
        request = RestRequest(raw_request)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_raw_url_args(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int})
        expected = Content(foo=[1, 3], bar=2)

        raw_request = MockedRequest()
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock:
            mock.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            request = RestRequest(raw_request)
            observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_raw_path_args(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int})
        expected = Content(foo=[1, 3], bar=2)

        raw_request = MockedRequest()
        with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock:
            mock.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            request = RestRequest(raw_request)
            observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_mixed_single(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int, "one": list[int], "two": int, "color": str})
        expected = Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="blue")

        raw_request = MockedRequest({"color": "blue"})
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock_url:
            mock_url.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock_path:
                mock_path.return_value = [("one", 1), ("two", 2), ("one", 3)]
                request = RestRequest(raw_request)
                observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_mixed(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int, "one": list[int], "two": int, "color": str})
        expected = [
            Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="blue"),
            Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="red"),
        ]

        raw_request = MockedRequest([{"color": "blue"}, {"color": "red"}])
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock_url:
            mock_url.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock_path:
                mock_path.return_value = [("one", 1), ("two", 2), ("one", 3)]
                request = RestRequest(raw_request)
                observed = await request.content()

        self.assertEqual(expected, observed)


class TestRestResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.models = [FakeModel("foo"), FakeModel("bar")]

    async def test_content(self):
        response = RestResponse(self.models)
        self.assertEqual([item.avro_data for item in self.models], await response.raw_content())

    async def test_content_single(self):
        response = RestResponse(self.models[0])
        self.assertEqual(self.models[0].avro_data, await response.raw_content())


if __name__ == "__main__":
    unittest.main()
