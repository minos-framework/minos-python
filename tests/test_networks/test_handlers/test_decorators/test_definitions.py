"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Request,
    Response,
)
from minos.networks import (
    MinosMultipleEnrouteDecoratorKindsException,
    enroute,
)
from minos.networks.handlers import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
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


class TestEnrouteDecorator(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = FakeRequest("test")
        self.decorator = enroute.broker.command("Create")

    def test_repr(self):
        self.assertEqual("BrokerCommandEnrouteDecorator(('Create',))", repr(self.decorator))

    async def test_method_call(self):
        instance = FakeService()
        response = await instance.create_ticket(FakeRequest("test"))
        self.assertEqual(Response("Create Ticket: test"), response)

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
        another = enroute.broker.event(topics=["CreateTicket"])
        with self.assertRaises(MinosMultipleEnrouteDecoratorKindsException):
            another(self.decorator(_fn))


class TestEnrouteDecoratorImplementations(unittest.IsolatedAsyncioTestCase):
    def test_rest_command(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        self.assertEqual(RestCommandEnrouteDecorator("tickets/", "GET"), decorator)

    def test_rest_query(self):
        decorator = enroute.rest.query(url="tickets/", method="GET")
        self.assertEqual(RestQueryEnrouteDecorator("tickets/", "GET"), decorator)

    def test_rest_event_raises(self):
        with self.assertRaises(AttributeError):
            enroute.rest.event(topics=["CreateTicket"])

    def test_broker_command_decorators(self):
        decorator = enroute.broker.command(topics=["CreateTicket"])
        self.assertEqual(BrokerCommandEnrouteDecorator("CreateTicket"), decorator)

    def test_broker_query_decorators(self):
        decorator = enroute.broker.query(topics=["CreateTicket"])
        self.assertEqual(BrokerQueryEnrouteDecorator("CreateTicket"), decorator)

    def test_broker_event_decorators(self):
        decorator = enroute.broker.event(topics=["CreateTicket"])
        self.assertEqual(BrokerEventEnrouteDecorator("CreateTicket"), decorator)


if __name__ == "__main__":
    unittest.main()
