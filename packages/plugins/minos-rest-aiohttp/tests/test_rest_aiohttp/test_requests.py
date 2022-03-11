import unittest
import warnings
from uuid import (
    uuid4,
)

from orjson import (
    orjson,
)

from minos.common import (
    MinosAvroProtocol,
    ModelType,
    classname,
)
from minos.networks import (
    NotHasContentException,
    NotHasParamsException,
    Response,
    ResponseException,
)
from minos.plugins.rest_aiohttp import (
    AioHttpRequest,
    AioHttpResponse,
    AioHttpResponseException,
)
from tests.utils import (
    FakeModel,
    avro_mocked_request,
    bytes_mocked_request,
    form_mocked_request,
    json_mocked_request,
    mocked_request,
    text_mocked_request,
)

APPLICATION_JSON = "application/json"
APPLICATION_URLENCODED = "application/x-www-form-urlencoded"
APPLICATION_OCTET_STREAM = "application/octet-stream"
TEXT_PLAIN = "text/plain"
AVRO_BINARY = "avro/binary"
IMAGE_PNG = "image/png"


class TestAioHttpRequest(unittest.IsolatedAsyncioTestCase):
    def test_raw(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        self.assertEqual(raw, request.raw)

    def test_raw_request(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(request.raw, request.raw_request)

    def test_repr(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        self.assertEqual(f"AioHttpRequest({raw!r})", repr(request))

    def test_eq_true(self):
        request = mocked_request()
        self.assertEqual(AioHttpRequest(request), AioHttpRequest(request))

    def test_eq_false(self):
        self.assertNotEqual(AioHttpRequest(mocked_request()), AioHttpRequest(mocked_request()))

    def test_headers(self):
        raw = mocked_request(headers={"something": "123"})
        request = AioHttpRequest(raw)
        self.assertEqual({"something": "123"}, request.headers)

    def test_user(self):
        uuid = uuid4()
        raw = mocked_request(user=uuid)
        request = AioHttpRequest(raw)
        user = request.user
        self.assertEqual(uuid, user)

    def test_user_unset(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        self.assertEqual(None, request.user)


class TestAioHttpRequestContent(unittest.IsolatedAsyncioTestCase):
    def test_has_content_false(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        self.assertEqual(False, request.has_content)

    def test_has_content_true(self):
        raw = json_mocked_request(123)
        request = AioHttpRequest(raw)
        self.assertEqual(True, request.has_content)

    async def test_empty_raises(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        with self.assertRaises(NotHasContentException):
            await request.content()

    async def test_json_int(self):
        expected = 56

        raw = json_mocked_request(56)
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_json_list(self):
        expected = [{"doors": 3, "color": "blue", "owner": None}, {"doors": 5, "color": "red", "owner": None}]

        raw = json_mocked_request(
            [{"doors": 3, "color": "blue", "owner": None}, {"doors": 5, "color": "red", "owner": None}]
        )
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_json_dict(self):
        expected = {"doors": 3, "color": "blue", "owner": None}

        raw = json_mocked_request({"doors": 3, "color": "blue", "owner": None})
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_form(self):
        expected = {"foo": "foo1", "bar": ["bar1", "bar2"]}

        raw = form_mocked_request({"foo": "foo1", "bar": ["bar1", "bar2"]})
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_int(self):
        expected = 56

        raw = avro_mocked_request(56, "int")
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_uuid(self):
        expected = uuid4()

        raw = avro_mocked_request(expected, {"type": "string", "logicalType": "uuid"})
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_dto(self):
        expected = ModelType.build("FakeModel", {"text": str})("foobar")

        raw = avro_mocked_request(
            {"text": "foobar"}, {"name": "FakeModel", "type": "record", "fields": [{"name": "text", "type": "string"}]}
        )
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_avro_model(self):
        expected = FakeModel("foobar")

        raw = avro_mocked_request(expected.avro_data, expected.avro_schema)
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_text(self):
        expected = "foobar"

        raw = text_mocked_request("foobar")
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_bytes(self):
        expected = bytes("foobar", "utf-8")

        raw = bytes_mocked_request(bytes("foobar", "utf-8"))
        request = AioHttpRequest(raw)
        observed = await request.content()

        self.assertEqual(expected, observed)

    async def test_raises(self):
        raw = mocked_request(content_type="foo/bar", data="foobar".encode())
        request = AioHttpRequest(raw)
        with self.assertRaises(ValueError):
            await request.content()

    async def test_with_type(self):
        # noinspection PyPep8Naming
        car = ModelType.build("Car", {"doors": int, "color": str, "owner": type(None)})
        expected = [car(3, "blue", None), car(5, "red", None)]

        raw = json_mocked_request([{"doors": "3", "color": "blue"}, {"doors": "5", "color": "red"}])
        request = AioHttpRequest(raw)
        observed = await request.content(type_=list[car])

        self.assertEqual(expected, observed)

    async def test_with_type_classname(self):
        expected = 3

        raw = json_mocked_request("3")
        request = AioHttpRequest(raw)
        observed = await request.content(type_=classname(int))

        self.assertEqual(expected, observed)

    async def test_with_model_type(self):
        expected = 3

        raw = json_mocked_request("3")
        request = AioHttpRequest(raw)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            observed = await request.content(model_type=classname(int))

        self.assertEqual(expected, observed)


class TestAioHttpRequestParams(unittest.IsolatedAsyncioTestCase):
    def test_has_url_params_false(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        self.assertEqual(False, request.has_url_params)

    def test_has_url_params_true(self):
        raw = mocked_request(url_params=[("foo", "1"), ("bar", "2"), ("foo", "3")])
        request = AioHttpRequest(raw)
        self.assertEqual(True, request.has_url_params)

    async def test_url_params(self):
        expected = {"bar": "2", "foo": ["1", "3"]}

        raw = mocked_request(url_params=[("foo", "1"), ("bar", "2"), ("foo", "3")])
        request = AioHttpRequest(raw)
        observed = await request.url_params()

        self.assertEqual(expected, observed)

    async def test_url_params_with_type(self):
        # noinspection PyPep8Naming
        params = ModelType.build("Params", {"foo": list[int], "bar": int})
        expected = params(foo=[1, 3], bar=2)

        raw = mocked_request(url_params=[("foo", "1"), ("bar", "2"), ("foo", "3")])
        request = AioHttpRequest(raw)
        observed = await request.url_params(type_=params)

        self.assertEqual(expected, observed)

    async def test_url_params_raises(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        with self.assertRaises(NotHasParamsException):
            await request.url_params()

    def test_has_query_params_false(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        self.assertEqual(False, request.has_query_params)

    def test_has_query_params_true(self):
        raw = mocked_request(query_params=[("one", "1"), ("two", "2"), ("one", "3")])
        request = AioHttpRequest(raw)
        self.assertEqual(True, request.has_query_params)

    async def test_query_params(self):
        expected = {"one": ["1", "3"], "two": "2"}

        raw = mocked_request(query_params=[("one", "1"), ("two", "2"), ("one", "3")])
        request = AioHttpRequest(raw)
        observed = await request.query_params()

        self.assertEqual(expected, observed)

    async def test_query_params_with_type(self):
        # noinspection PyPep8Naming
        params = ModelType.build("Params", {"one": list[int], "two": int})
        expected = params(one=[1, 3], two=2)

        raw = mocked_request(query_params=[("one", "1"), ("two", "2"), ("one", "3")])
        request = AioHttpRequest(raw)
        observed = await request.query_params(type_=params)

        self.assertEqual(expected, observed)

    async def test_query_params_raises(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        with self.assertRaises(NotHasParamsException):
            await request.query_params()

    def test_has_params_false(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        self.assertEqual(False, request.has_params)

    def test_has_params_true(self):
        raw = mocked_request(url_params=[("foo", "1"), ("bar", "2"), ("foo", "3")])
        request = AioHttpRequest(raw)
        self.assertEqual(True, request.has_params)

        raw = mocked_request(query_params=[("one", "1"), ("two", "2"), ("one", "3")])
        request = AioHttpRequest(raw)
        self.assertEqual(True, request.has_params)

        raw = mocked_request(
            url_params=[("foo", "1"), ("bar", "2"), ("foo", "3")],
            query_params=[("one", "1"), ("two", "2"), ("one", "3")],
        )
        request = AioHttpRequest(raw)
        self.assertEqual(True, request.has_params)

    async def test_params(self):
        expected = {"bar": "2", "foo": ["1", "3"], "one": ["1", "3"], "two": "2"}

        raw = mocked_request(
            url_params=[("foo", "1"), ("bar", "2"), ("foo", "3")],
            query_params=[("one", "1"), ("two", "2"), ("one", "3")],
        )
        request = AioHttpRequest(raw)
        observed = await request.params()

        self.assertEqual(expected, observed)

    async def test_params_with_type(self):
        # noinspection PyPep8Naming
        params = ModelType.build("Params", {"foo": list[int], "bar": int, "one": list[int], "two": int})
        expected = params(foo=[1, 3], bar=2, one=[1, 3], two=2)

        raw = mocked_request(
            url_params=[("foo", "1"), ("bar", "2"), ("foo", "3")],
            query_params=[("one", "1"), ("two", "2"), ("one", "3")],
        )
        request = AioHttpRequest(raw)
        observed = await request.params(type_=params)

        self.assertEqual(expected, observed)

    async def test_params_raises(self):
        raw = mocked_request()
        request = AioHttpRequest(raw)
        with self.assertRaises(NotHasParamsException):
            await request.params()


class TestAioHttpResponse(unittest.IsolatedAsyncioTestCase):
    async def test_from_response(self):
        base = Response("foobar")

        response = AioHttpResponse.from_response(base)

        self.assertEqual(orjson.dumps("foobar"), await response.content())
        self.assertEqual(APPLICATION_JSON, response.content_type)

    def test_from_response_already(self):
        base = AioHttpResponse()

        response = AioHttpResponse.from_response(base)

        self.assertEqual(base, response)

    def test_from_response_empty(self):
        response = AioHttpResponse.from_response(None)

        self.assertEqual(AioHttpResponse(), response)

    async def test_empty(self):
        response = AioHttpResponse()
        self.assertEqual(None, await response.content())
        self.assertEqual(APPLICATION_JSON, response.content_type)

    async def test_content_json(self):
        data = [FakeModel("foo"), FakeModel("bar")]
        response = AioHttpResponse(data)
        self.assertEqual(orjson.dumps([item.avro_data for item in data]), await response.content())
        self.assertEqual(APPLICATION_JSON, response.content_type)

    async def test_content_form(self):
        data = {"foo": "bar", "one": "two"}
        response = AioHttpResponse(data, content_type="application/x-www-form-urlencoded")
        self.assertEqual("foo=bar&one=two".encode(), await response.content())
        self.assertEqual(APPLICATION_URLENCODED, response.content_type)

    async def test_content_bytes(self):
        data = "foobar".encode()
        response = AioHttpResponse(data, content_type=APPLICATION_OCTET_STREAM)
        self.assertEqual("foobar".encode(), await response.content())
        self.assertEqual(APPLICATION_OCTET_STREAM, response.content_type)

    async def test_content_bytes_raises(self):
        data = 56
        response = AioHttpResponse(data, content_type=APPLICATION_OCTET_STREAM)
        with self.assertRaises(ValueError):
            await response.content()

    async def test_content_text(self):
        data = "foobar"
        response = AioHttpResponse(data, content_type=TEXT_PLAIN)
        self.assertEqual("foobar".encode(), await response.content())
        self.assertEqual(TEXT_PLAIN, response.content_type)

    async def test_content_text_raises(self):
        data = 56
        response = AioHttpResponse(data, content_type=TEXT_PLAIN)
        with self.assertRaises(ValueError):
            await response.content()

    async def test_content_avro(self):
        data = "foobar"
        response = AioHttpResponse(data, content_type=AVRO_BINARY)
        self.assertEqual(data, MinosAvroProtocol.decode(await response.content()))
        self.assertEqual(AVRO_BINARY, response.content_type)

    async def test_content_image(self):
        data = bytes("image", "utf-8")
        response = AioHttpResponse(data, content_type=IMAGE_PNG)
        self.assertEqual(data, await response.content())
        self.assertEqual(IMAGE_PNG, response.content_type)


class TestAioHttpResponseException(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(AioHttpResponseException, ResponseException))


if __name__ == "__main__":
    unittest.main()
