import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    BrokerMessage,
    BrokerRequest,
    BrokerResponse,
    NotHasParamsException,
)
from tests.utils import (
    FakeModel,
)


class TestBrokerRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data = [FakeModel("foo"), FakeModel("bar")]
        self.identifier = uuid4()
        self.raw = BrokerMessage("FooCreated", self.data)
        self.request = BrokerRequest(self.raw)

    def test_repr(self):
        expected = f"BrokerRequest({self.raw!r})"
        self.assertEqual(expected, repr(self.request))

    def test_eq_true(self):
        self.assertEqual(self.request, BrokerRequest(self.raw))

    def test_eq_false(self):
        self.assertNotEqual(self.request, BrokerRequest(BrokerMessage("FooUpdated", self.data)))

    def test_user(self):
        raw = BrokerMessage("FooCreated", self.data, user=uuid4())
        request = BrokerRequest(raw)
        self.assertEqual(raw.user, request.user)

    def test_user_unset(self):
        self.assertEqual(None, self.request.user)

    def test_raw(self):
        self.assertEqual(self.raw, self.request.raw)

    def test_has_content(self):
        self.assertEqual(True, self.request.has_content)

    async def test_content(self):
        self.assertEqual(self.data, await self.request.content())

    def test_has_params(self):
        self.assertEqual(False, self.request.has_params)

    async def test_params_raises(self):
        with self.assertRaises(NotHasParamsException):
            await self.request.params()


class TestHandlerResponse(unittest.IsolatedAsyncioTestCase):
    async def test_content(self):
        response = BrokerResponse([FakeModel("foo"), FakeModel("bar")])
        self.assertEqual([FakeModel("foo"), FakeModel("bar")], await response.content())

    async def test_content_single(self):
        response = BrokerResponse(FakeModel("foo"))
        self.assertEqual(FakeModel("foo"), await response.content())


if __name__ == "__main__":
    unittest.main()
