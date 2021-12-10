import unittest
from typing import (
    Iterable,
)

from minos.networks import (
    EnrouteHandleDecorator,
    EnrouteHandleDecoratorKind,
    HandlerMeta,
    HandlerProtocol,
    MinosMultipleEnrouteDecoratorKindsException,
    Request,
    Response,
    enroute,
)
from tests.utils import (
    FakeRequest,
    FakeService,
)


# noinspection PyUnusedLocal
def _fn(request: Request) -> Response:
    """For testing purposes."""
    return Response("Fn")


async def _async_fn(request: Request) -> Response:
    """For testing purposes."""
    return Response(f"Async Fn: {await request.content()}")


class _FakeEnrouteHandleDecorator(EnrouteHandleDecorator):
    KIND = EnrouteHandleDecoratorKind.Command

    def __iter__(self) -> Iterable:
        yield from []


class TestEnrouteHandleDecorator(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = FakeRequest("test")
        self.decorator = _FakeEnrouteHandleDecorator()

    def test_decorate(self):
        decorated = self.decorator(_fn)
        self.assertIsInstance(decorated, HandlerProtocol)
        self.assertEqual(HandlerMeta(_fn, {self.decorator}), decorated.meta)

    def test_iter(self):
        self.assertEqual(tuple(), tuple(self.decorator))

    def test_hash(self):
        self.assertEqual(hash(tuple()), hash(self.decorator))

    def test_repr(self):
        self.assertEqual("_FakeEnrouteHandleDecorator()", repr(self.decorator))

    def test_method_call(self):
        instance = FakeService()
        response = instance.create_ticket(FakeRequest("test"))
        self.assertEqual(Response("Create Ticket"), response)

    async def test_static_method_call(self):
        instance = FakeService()
        response = await instance.ticket_added(FakeRequest("test"))
        self.assertEqual(Response("Ticket Added: test"), response)

    def test_function_call(self):
        wrapper = self.decorator(_fn)
        self.assertEqual(Response("Fn"), wrapper(self.request))

    async def test_async_function_call(self):
        wrapper = self.decorator(_async_fn)
        self.assertEqual(Response("Async Fn: test"), await wrapper(self.request))

    def test_multiple_decorator_kind_raises(self):
        another = enroute.broker.event("CreateTicket")
        with self.assertRaises(MinosMultipleEnrouteDecoratorKindsException):
            another(self.decorator(_fn))

    def test_pre_fn_name(self):
        self.assertEqual("_pre_command_handle", self.decorator.pre_fn_name)

    def test_post_fn_name(self):
        self.assertEqual("_post_command_handle", self.decorator.post_fn_name)


if __name__ == "__main__":
    unittest.main()
