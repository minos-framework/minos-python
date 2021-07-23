"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Request,
    Response,
    classname,
)
from minos.networks import (
    EnrouteBuilder,
    EnrouteDecoratorAnalyzer,
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


class FakeDecorated:
    """For testing purposes."""

    # noinspection PyMethodMayBeStatic
    async def _pre_command_handle(self, request: Request) -> Request:
        return request

    # noinspection PyMethodMayBeStatic
    async def _pre_query_handle(self, request: Request) -> Request:
        return request

    # noinspection PyMethodMayBeStatic
    async def _pre_event_handle(self, request: Request) -> Request:
        return request

    @enroute.rest.command(url="orders/", method="GET")
    @enroute.broker.command(topics=["CreateTicket", "AddTicket"])
    async def create_ticket(self, request: Request) -> Response:
        """For testing purposes."""
        return Response(await request.content())

    @enroute.rest.query(url="tickets/", method="GET")
    @enroute.broker.query(topics=["GetTickets"])
    async def get_tickets(self, request: Request) -> Response:
        """For testing purposes."""
        return Response(await request.content())

    @staticmethod
    @enroute.broker.event(topics=["TicketAdded"])
    async def ticket_added(request: Request) -> Response:
        """For testing purposes."""
        return Response(await request.content())

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def bar(self, request: Request):
        """For testing purposes."""
        return Response("bar")


# noinspection PyUnusedLocal
def _fn(request: Request) -> Response:
    """For testing purposes."""
    return Response("test")


async def _async_fn(request: Request) -> Response:
    """For testing purposes."""
    return Response(await request.content())


class _FakeRequest(Request):
    """For testing purposes"""

    def __init__(self, content):
        super().__init__()
        self._content = content

    async def content(self, **kwargs):
        """For testing purposes"""
        return self._content

    def __eq__(self, other) -> bool:
        return self._content == other._content

    def __repr__(self) -> str:
        return str()


class TestEnrouteDecorator(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = _FakeRequest("test")
        self.decorator = enroute.broker.command("Create")

    def test_repr(self):
        self.assertEqual("BrokerCommandEnrouteDecorator(('Create',))", repr(self.decorator))

    async def test_method_command_call(self):
        instance = FakeDecorated()
        response = await instance.create_ticket(_FakeRequest("test"))
        self.assertEqual(Response("test"), response)

    async def test_method_query_call(self):
        instance = FakeDecorated()
        response = await instance.get_tickets(_FakeRequest("test"))
        self.assertEqual(Response("test"), response)

    async def test_method_query_call_2(self):
        # FIXME
        builder = EnrouteBuilder(FakeDecorated)
        fn = builder.get_broker_event()[0][0]

        response = await fn(_FakeRequest("test"))
        self.assertEqual(Response("test"), response)

    async def test_static_method_event_call(self):
        instance = FakeDecorated()
        response = await instance.ticket_added(_FakeRequest("test"))
        self.assertEqual(Response("test"), response)

    def test_function_command_call(self):
        wrapper = self.decorator(_fn)
        self.assertEqual(Response("test"), wrapper(self.request))

    async def test_fn_async(self):
        wrapper = self.decorator(_async_fn)
        self.assertEqual(Response("test"), await wrapper(self.request))

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


class TestEnrouteDecoratorAnalyzer(unittest.IsolatedAsyncioTestCase):
    def test_decorated_str(self):
        analyzer = EnrouteDecoratorAnalyzer(classname(FakeDecorated))
        self.assertEqual(FakeDecorated, analyzer.decorated)

    def test_multiple_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_all()
        expected = {
            "get_tickets": {
                BrokerQueryEnrouteDecorator(("GetTickets",)),
                RestQueryEnrouteDecorator("tickets/", "GET"),
            },
            "create_ticket": {
                BrokerCommandEnrouteDecorator(("CreateTicket", "AddTicket")),
                RestCommandEnrouteDecorator("orders/", "GET"),
            },
            "ticket_added": {BrokerEventEnrouteDecorator("TicketAdded")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_rest_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_rest_command_query()
        expected = {
            "get_tickets": {RestQueryEnrouteDecorator("tickets/", "GET")},
            "create_ticket": {RestCommandEnrouteDecorator("orders/", "GET")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_command_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_broker_command_query()
        expected = {
            "get_tickets": {BrokerQueryEnrouteDecorator("GetTickets")},
            "create_ticket": {BrokerCommandEnrouteDecorator(("CreateTicket", "AddTicket"))},
        }

        self.assertEqual(expected, observed)

    def test_get_only_event_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_broker_event()
        expected = {"ticket_added": {BrokerEventEnrouteDecorator("TicketAdded")}}

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
