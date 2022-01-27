import unittest
import warnings
from uuid import (
    uuid4,
)

from minos.networks import (
    REQUEST_USER_CONTEXT_VAR,
    InMemoryRequest,
)


class TestMemoryRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.user = uuid4()
        self.content = "hello"
        self.params = {"hello": "world"}
        self.request = InMemoryRequest(self.content, self.params, self.user)

    def test_equal_true(self):
        self.assertEqual(InMemoryRequest(self.content, self.params, self.user), self.request)

    def test_repr(self):
        self.assertEqual(f"InMemoryRequest({self.content!r}, {self.params!r}, {self.user!r})", repr(self.request))

    def test_user(self):
        self.assertEqual(self.user, self.request.user)

    async def test_user_from_context_var(self):
        REQUEST_USER_CONTEXT_VAR.set(self.user)
        self.assertEqual(self.user, InMemoryRequest().user)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            self.assertEqual(self.user, InMemoryRequest(user=uuid4()).user)

    def test_has_content_true(self):
        self.assertEqual(True, self.request.has_content)

    def test_has_content_false(self):
        self.assertEqual(False, InMemoryRequest().has_content)

    async def test_content(self):
        self.assertEqual(self.content, await self.request.content())

    def test_has_params(self):
        self.assertEqual(True, self.request.has_params)

    def test_has_params_false(self):
        self.assertEqual(False, InMemoryRequest().has_params)

    async def test_params(self):
        self.assertEqual(self.params, await self.request.params())


if __name__ == "__main__":
    unittest.main()
