import unittest
from unittest.mock import (
    PropertyMock,
    patch,
)
from uuid import (
    uuid4,
)

from minos.common import (
    ModelType,
)
from minos.networks import (
    RestRequest,
    RestResponse,
)
from tests.test_networks.test_rest.test_handlers import (
    mocked_request,
)
from tests.utils import (
    FakeModel,
)


class TestRestRequest(unittest.IsolatedAsyncioTestCase):
    def test_raw_request(self):
        raw = mocked_request()
        request = RestRequest(raw)
        self.assertEqual(raw, request.raw)

    def test_repr(self):
        raw = mocked_request()
        request = RestRequest(raw)
        self.assertEqual(f"RestRequest({raw!r})", repr(request))

    def test_eq_true(self):
        request = mocked_request()
        self.assertEqual(RestRequest(request), RestRequest(request))

    def test_eq_false(self):
        self.assertNotEqual(RestRequest(mocked_request()), RestRequest(mocked_request()))

    def test_headers(self):
        uuid = uuid4()
        raw = mocked_request(user=uuid)
        request = RestRequest(raw)
        self.assertEqual({"User": str(uuid), "something": "123"}, request.headers)

    def test_user(self):
        uuid = uuid4()
        raw = mocked_request(user=uuid)
        request = RestRequest(raw)
        user = request.user
        self.assertEqual(uuid, user)

    async def test_content(self):
        Content = ModelType.build(
            "Content", {"id": int, "version": int, "doors": int, "color": str, "owner": type(None)}
        )
        expected = [
            Content(id=1, version=1, doors=3, color="blue", owner=None),
            Content(id=2, version=1, doors=5, color="red", owner=None),
        ]

        raw = mocked_request(
            [
                {"id": 1, "version": 1, "doors": 3, "color": "blue", "owner": None},
                {"id": 2, "version": 1, "doors": 5, "color": "red", "owner": None},
            ]
        )
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_single(self):
        Content = ModelType.build(
            "Content", {"id": int, "version": int, "doors": int, "color": str, "owner": type(None)}
        )
        expected = Content(id=1, version=1, doors=3, color="blue", owner=None)

        raw = mocked_request({"id": 1, "version": 1, "doors": 3, "color": "blue", "owner": None})
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_raw_url_args(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int})
        expected = Content(foo=[1, 3], bar=2)

        raw = mocked_request()
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock:
            mock.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            request = RestRequest(raw)
            observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_raw_path_args(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int})
        expected = Content(foo=[1, 3], bar=2)

        raw = mocked_request()
        with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock:
            mock.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            request = RestRequest(raw)
            observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_mixed_single(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int, "one": list[int], "two": int, "color": str})
        expected = Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="blue")

        raw = mocked_request({"color": "blue"})
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock_url:
            mock_url.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock_path:
                mock_path.return_value = [("one", 1), ("two", 2), ("one", 3)]
                request = RestRequest(raw)
                observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_content_mixed(self):
        Content = ModelType.build("Content", {"foo": list[int], "bar": int, "one": list[int], "two": int, "color": str})
        expected = [
            Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="blue"),
            Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="red"),
        ]

        raw = mocked_request([{"color": "blue"}, {"color": "red"}])
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock_url:
            mock_url.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock_path:
                mock_path.return_value = [("one", 1), ("two", 2), ("one", 3)]
                request = RestRequest(raw)
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
