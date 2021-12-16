import unittest
from abc import (
    ABC,
)

from minos.networks import (
    Request,
    Response,
    WrappedRequest,
)
from tests.utils import (
    FakeModel,
    FakeRequest,
)


class TestRequest(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(Request, ABC))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"__eq__", "__repr__", "content", "has_content", "has_params", "params", "user"},
            Request.__abstractmethods__,
        )


async def _content_action(content: str) -> str:
    return f"Wrapped Content: {content}"


async def _params_action(params: str) -> str:
    return f"Wrapped Params: {params}"


class TestWrappedRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.base = FakeRequest("hello", "world")
        self.request = WrappedRequest(self.base, _content_action, _params_action)

    def test_equal_true(self):
        self.assertEqual(WrappedRequest(self.base, _content_action, _params_action), self.request)

    def test_equal_false(self):
        self.assertNotEqual(WrappedRequest(FakeRequest("foo"), _content_action, _params_action), self.request)

    def test_repr(self):
        self.assertEqual(f"WrappedRequest({self.base!r}, {_content_action!r}, {_params_action!r})", repr(self.request))

    def test_user(self):
        self.assertEqual(self.base.user, self.request.user)

    def test_has_content(self):
        self.assertEqual(self.base.has_content, self.request.has_content)

    async def test_content(self):
        expected = "Wrapped Content: hello"
        self.assertEqual(expected, await self.request.content())

    async def test_content_without_action(self):
        expected = "hello"
        self.assertEqual(expected, await WrappedRequest(self.base).content())

    def test_has_params(self):
        self.assertEqual(self.base.has_params, self.request.has_params)

    async def test_params(self):
        expected = "Wrapped Params: world"
        self.assertEqual(expected, await self.request.params())

    async def test_params_without_action(self):
        expected = "world"
        self.assertEqual(expected, await WrappedRequest(self.base).params())


class TestResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data = [FakeModel("blue"), FakeModel("red")]

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
        self.assertEqual(f"Response({self.data!r})", repr(response))

    def test_hash(self):
        self.assertIsInstance(hash(Response("test")), int)


if __name__ == "__main__":
    unittest.main()
