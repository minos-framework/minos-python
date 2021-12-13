import unittest
import warnings
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
from tests.test_networks.test_rest.utils import (
    avro_mocked_request,
    bytes_mocked_request,
    form_mocked_request,
    json_mocked_request,
    mocked_request,
    text_mocked_request,
)
from tests.utils import (
    FakeModel,
)


class TestRestRequest(unittest.IsolatedAsyncioTestCase):
    def test_raw(self):
        raw = mocked_request()
        request = RestRequest(raw)
        self.assertEqual(raw, request.raw)

    def test_raw_request(self):
        raw = mocked_request()
        request = RestRequest(raw)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(request.raw, request.raw_request)

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
        raw = mocked_request(user=uuid, headers={"something": "123"})
        request = RestRequest(raw)
        self.assertEqual({"User": str(uuid), "something": "123"}, request.headers)

    def test_user(self):
        uuid = uuid4()
        raw = mocked_request(user=uuid)
        request = RestRequest(raw)
        user = request.user
        self.assertEqual(uuid, user)

    def test_user_unset(self):
        raw = mocked_request()
        request = RestRequest(raw)
        self.assertEqual(None, request.user)


class TestRestRequestContent(unittest.IsolatedAsyncioTestCase):
    async def test_json_int(self):
        expected = 56

        raw = json_mocked_request(56)
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_json_list(self):
        # noinspection PyPep8Naming
        Content = ModelType.build("Content", {"doors": int, "color": str, "owner": type(None)})
        expected = [Content(3, "blue", None), Content(5, "red", None)]

        raw = json_mocked_request(
            [{"doors": 3, "color": "blue", "owner": None}, {"doors": 5, "color": "red", "owner": None}]
        )
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_json_dict(self):
        expected = ModelType.build("Content", {"doors": int, "color": str, "owner": type(None)})(3, "blue", None)

        raw = json_mocked_request({"doors": 3, "color": "blue", "owner": None})
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_form(self):
        expected = ModelType.build("Content", {"foo": str, "bar": list[str]})("foo1", ["bar1", "bar2"])

        raw = form_mocked_request({"foo": "foo1", "bar": ["bar1", "bar2"]})
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_int(self):
        expected = 56

        raw = avro_mocked_request(56, "int")
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_uuid(self):
        expected = uuid4()

        raw = avro_mocked_request(expected, {"type": "string", "logicalType": "uuid"})
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_dto(self):
        expected = ModelType.build("FakeModel", {"text": str})("foobar")

        raw = avro_mocked_request(
            {"text": "foobar"}, {"name": "FakeModel", "type": "record", "fields": [{"name": "text", "type": "string"}]}
        )
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_model(self):
        expected = FakeModel("foobar")

        raw = avro_mocked_request(expected.avro_data, expected.avro_schema)
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_text(self):
        expected = "foobar"

        raw = text_mocked_request("foobar")
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_bytes(self):
        expected = bytes("foobar", "utf-8")

        raw = bytes_mocked_request(bytes("foobar", "utf-8"))
        request = RestRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_raises(self):
        raw = mocked_request(content_type="foo/bar")
        request = RestRequest(raw)
        with self.assertRaises(ValueError):
            await request.content()


class TestRestRequestContentArgs(unittest.IsolatedAsyncioTestCase):
    async def test_url_args(self):
        expected = ModelType.build("Content", {"foo": list[int], "bar": int})(foo=[1, 3], bar=2)

        raw = mocked_request()
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock:
            mock.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            request = RestRequest(raw)
            observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_path_args(self):
        expected = ModelType.build("Content", {"foo": list[int], "bar": int})(foo=[1, 3], bar=2)

        raw = mocked_request()
        with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock:
            mock.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            request = RestRequest(raw)
            observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_int_with_args(self):
        expected = 56

        raw = json_mocked_request(56)
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock_url:
            mock_url.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock_path:
                mock_path.return_value = [("one", 1), ("two", 2), ("one", 3)]
                request = RestRequest(raw)
                observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_dict_with_args(self):
        expected = ModelType.build(
            "Content", {"foo": list[int], "bar": int, "one": list[int], "two": int, "color": str}
        )(foo=[1, 3], bar=2, one=[1, 3], two=2, color="blue")

        raw = json_mocked_request({"color": "blue"})
        with patch("minos.networks.RestRequest._raw_url_args", new_callable=PropertyMock) as mock_url:
            mock_url.return_value = [("foo", 1), ("bar", 2), ("foo", 3)]
            with patch("minos.networks.RestRequest._raw_path_args", new_callable=PropertyMock) as mock_path:
                mock_path.return_value = [("one", 1), ("two", 2), ("one", 3)]
                request = RestRequest(raw)
                observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_list_with_args(self):
        # noinspection PyPep8Naming
        Content = ModelType.build("Content", {"foo": list[int], "bar": int, "one": list[int], "two": int, "color": str})
        expected = [
            Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="blue"),
            Content(foo=[1, 3], bar=2, one=[1, 3], two=2, color="red"),
        ]

        raw = json_mocked_request([{"color": "blue"}, {"color": "red"}])
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
