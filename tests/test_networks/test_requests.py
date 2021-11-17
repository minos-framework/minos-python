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
        self.assertEqual({"user", "content", "__eq__", "__repr__"}, Request.__abstractmethods__)


async def _action(content: str) -> str:
    return f"Wrapped Request: {content}"


class TestWrappedRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.base = FakeRequest("hello")
        self.action = _action
        self.request = WrappedRequest(self.base, self.action)

    async def test_content(self):
        expected = "Wrapped Request: hello"
        self.assertEqual(expected, await self.request.content())

    def test_equal_true(self):
        self.assertEqual(WrappedRequest(self.base, self.action), self.request)

    def test_equal_false(self):
        self.assertNotEqual(WrappedRequest(FakeRequest("foo"), self.action), self.request)

    def test_repr(self):
        self.assertEqual(f"WrappedRequest({self.base!r}, {self.action!r})", repr(self.request))

    def test_user(self):
        self.assertEqual(self.base.user, self.request.user)


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
