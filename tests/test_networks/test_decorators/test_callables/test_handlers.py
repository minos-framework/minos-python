import unittest
from unittest.mock import (
    call,
    patch,
)

from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    HandlerMeta,
    HandlerProtocol,
    MinosMultipleEnrouteDecoratorKindsException,
    NotSatisfiedCheckerException,
    Request,
    Response,
    ResponseException,
)
# noinspection PyUnusedLocal
from tests.utils import (
    FakeRequest,
)


def _fn(request: Request) -> Response:
    """For testing purposes."""
    return Response("Fn")


async def _async_fn(request: Request) -> Response:
    """For testing purposes."""
    return Response(f"Async Fn: {await request.content()}")


class TestHandlerMeta(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.meta = HandlerMeta(_fn)

    def test_constructor(self):
        self.assertEqual(_fn, self.meta.func)
        self.assertEqual(set(), self.meta.decorators)
        self.assertEqual(set(), self.meta.checkers)

    async def test_wrapper_async(self):
        meta = HandlerMeta(_async_fn)
        wrapper = meta.wrapper
        self.assertIsInstance(wrapper, HandlerProtocol)
        self.assertEqual(meta, wrapper.meta)
        self.assertEqual(meta.check, wrapper.check)

    def test_wrapper_sync(self):
        meta = HandlerMeta(_fn)
        wrapper = meta.wrapper
        self.assertIsInstance(wrapper, HandlerProtocol)
        self.assertEqual(meta, wrapper.meta)
        self.assertEqual(meta.check, wrapper.check)

    async def test_wrapper_async_call(self):
        meta = HandlerMeta(_async_fn)
        with patch("minos.networks.CheckerMeta.run_async") as mock:
            self.assertEqual(Response("Async Fn: foo"), await meta.wrapper(FakeRequest("foo")))
            self.assertEqual([call(meta.decorators, FakeRequest("foo"))], mock.call_args_list)

    async def test_wrapper_async_call_raises(self):
        meta = HandlerMeta(_async_fn)
        with patch("minos.networks.CheckerMeta.run_async", side_effect=NotSatisfiedCheckerException("")):
            with self.assertRaises(ResponseException):
                await meta.wrapper(FakeRequest("foo"))

    def test_wrapper_sync_call(self):
        meta = HandlerMeta(_fn)
        with patch("minos.networks.CheckerMeta.run_sync") as mock:
            self.assertEqual(Response("Fn"), meta.wrapper(FakeRequest("foo")))
            self.assertEqual([call(meta.decorators, FakeRequest("foo"))], mock.call_args_list)

    async def test_wrapper_sync_call_raises(self):
        meta = HandlerMeta(_fn)
        with patch("minos.networks.CheckerMeta.run_sync", side_effect=NotSatisfiedCheckerException("")):
            with self.assertRaises(ResponseException):
                await meta.wrapper(FakeRequest("foo"))

    def test_add_decorator(self):
        decorator = BrokerCommandEnrouteDecorator("CreateFoo")
        self.meta.add_decorator(decorator)
        self.assertEqual({decorator}, self.meta.decorators)

    def test_add_decorator_raises(self):
        self.meta.add_decorator(BrokerCommandEnrouteDecorator("CreateFoo"))

        with self.assertRaises(MinosMultipleEnrouteDecoratorKindsException):
            self.meta.add_decorator(BrokerQueryEnrouteDecorator("GetFoo"))

    def test_repr(self):
        self.assertEqual(f"HandlerMeta({_fn!r}, {set()!r}, {set()!r})", repr(self.meta))

    def test_eq(self):
        self.assertEqual(HandlerMeta(_fn), HandlerMeta(_fn))
        self.assertNotEqual(HandlerMeta(_fn), HandlerMeta(_async_fn))

    def test_iter(self):
        self.assertEqual((_fn, set(), set()), tuple(self.meta))

    def test_hash(self):
        self.assertEqual(hash(_fn), hash(self.meta))


if __name__ == "__main__":
    unittest.main()
