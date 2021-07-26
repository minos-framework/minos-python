"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    Iterable,
)

from minos.common import (
    Request,
    Response,
)
from minos.networks import (
    MinosMultipleEnrouteDecoratorKindsException,
    enroute,
)
from minos.networks.handlers.decorators import (
    EnrouteDecorator,
    EnrouteDecoratorKind,
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


class _FakeEnrouteDecorator(EnrouteDecorator):
    KIND = EnrouteDecoratorKind.Command

    def __iter__(self) -> Iterable:
        yield from []


class TestEnrouteDecorator(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = FakeRequest("test")
        self.decorator = _FakeEnrouteDecorator()

    def test_repr(self):
        self.assertEqual("_FakeEnrouteDecorator()", repr(self.decorator))

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


if __name__ == "__main__":
    unittest.main()
