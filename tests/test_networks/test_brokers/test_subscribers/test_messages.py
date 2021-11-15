import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    Command,
    HandlerRequest,
    HandlerResponse,
)
from tests.utils import (
    FakeModel,
)


class TestHandlerRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data = [FakeModel("foo"), FakeModel("bar")]
        self.saga = uuid4()
        self.raw = Command("FooCreated", self.data, saga=self.saga, reply_topic="AddOrderReply")

    def test_repr(self):
        request = HandlerRequest(self.raw)
        expected = f"HandlerRequest({self.raw!r})"
        self.assertEqual(expected, repr(request))

    def test_eq_true(self):
        self.assertEqual(HandlerRequest(self.raw), HandlerRequest(self.raw))

    def test_eq_false(self):
        another = HandlerRequest(Command("FooUpdated", self.data, saga=self.saga, reply_topic="AddOrderReply"))
        self.assertNotEqual(HandlerRequest(self.raw), another)

    def test_no_user(self):
        request = HandlerRequest(self.raw)
        self.assertEqual(None, request.user)

    def test_command(self):
        request = HandlerRequest(self.raw)
        self.assertEqual(self.raw, request.raw)

    async def test_content(self):
        request = HandlerRequest(self.raw)
        self.assertEqual(self.data, await request.content())

    async def test_content_single(self):
        request = HandlerRequest(Command("FooCreated", self.data[0], saga=self.saga, reply_topic="AddOrderReply"))
        self.assertEqual(self.data[0], await request.content())

    async def test_content_simple(self):
        request = HandlerRequest(Command("FooCreated", 1234, saga=self.saga, reply_topic="AddOrderReply"))
        self.assertEqual(1234, await request.content())


class TestHandlerResponse(unittest.IsolatedAsyncioTestCase):
    async def test_content(self):
        response = HandlerResponse([FakeModel("foo"), FakeModel("bar")])
        self.assertEqual([FakeModel("foo"), FakeModel("bar")], await response.content())

    async def test_content_single(self):
        response = HandlerResponse(FakeModel("foo"))
        self.assertEqual(FakeModel("foo"), await response.content())


if __name__ == "__main__":
    unittest.main()
