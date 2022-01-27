import unittest
from typing import (
    Any,
)

from minos.networks import (
    InMemoryRequest,
    WrappedRequest,
)


async def _content_action(content: str) -> str:
    return f"Wrapped Content: {content}"


async def _params_action(params: dict[str, Any]) -> dict[str, Any]:
    return params | {"wrapped": "params"}


class TestWrappedRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.base = InMemoryRequest("hello", {"hello": "world"})
        self.request = WrappedRequest(self.base, _content_action, _params_action)

    def test_equal_true(self):
        self.assertEqual(WrappedRequest(self.base, _content_action, _params_action), self.request)

    def test_equal_false(self):
        self.assertNotEqual(WrappedRequest(InMemoryRequest("foo"), _content_action, _params_action), self.request)

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
        expected = {"hello": "world", "wrapped": "params"}
        self.assertEqual(expected, await self.request.params())

    async def test_params_without_action(self):
        expected = {"hello": "world"}
        self.assertEqual(expected, await WrappedRequest(self.base).params())


if __name__ == "__main__":
    unittest.main()
