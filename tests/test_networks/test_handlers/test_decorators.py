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
    def _pre_command_handle(self, request: Request) -> Request:
        return request

    # noinspection PyMethodMayBeStatic
    def _pre_query_handle(self, request: Request) -> Request:
        return request

    # noinspection PyMethodMayBeStatic
    def _pre_event_handle(self, request: Request) -> Request:
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

    @enroute.broker.event(topics=["TicketAdded"])
    async def ticket_added(self, request: Request) -> Response:
        """For testing purposes."""
        return Response(await request.content())

    # noinspection PyMethodMayBeStatic
    def bar(self):
        """For testing purposes."""
        return "bar"


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


class TestEnroute(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = _FakeRequest("test")

    def test_repr(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        self.assertEqual("RestCommandEnrouteDecorator('tickets/', 'GET')", repr(decorator))

    def test_rest_command(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        wrapper = decorator(_fn)
        self.assertEqual(Response("test"), wrapper(self.request))

    async def test_rest_command_async(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        wrapper = decorator(_async_fn)
        self.assertEqual(Response("test"), await wrapper(self.request))

    def test_rest_command_decorators(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        wrapper = decorator(_fn)
        self.assertEqual({RestCommandEnrouteDecorator("tickets/", "GET")}, wrapper.__decorators__)

    def test_rest_query(self):
        decorator = enroute.rest.query(url="tickets/", method="GET")
        wrapper = decorator(_fn)
        self.assertEqual(Response("test"), wrapper(self.request))

    async def test_rest_query_async(self):
        decorator = enroute.rest.query(url="tickets/", method="GET")
        wrapper = decorator(_async_fn)
        self.assertEqual(Response("test"), await wrapper(self.request))

    def test_rest_query_decorators(self):
        decorator = enroute.rest.query(url="tickets/", method="GET")
        wrapper = decorator(_fn)
        self.assertEqual({RestQueryEnrouteDecorator("tickets/", "GET")}, wrapper.__decorators__)

    def test_rest_event_raises(self):
        with self.assertRaises(AttributeError):
            enroute.rest.event(topics=["CreateTicket"])

    def test_broker_command(self):
        decorator = enroute.broker.command(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual(Response("test"), wrapper(self.request))

    async def test_broker_command_async(self):
        decorator = enroute.broker.command(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual(Response("test"), await wrapper(self.request))

    def test_broker_command_decorators(self):
        decorator = enroute.broker.command(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual({BrokerCommandEnrouteDecorator("CreateTicket")}, wrapper.__decorators__)

    def test_broker_query(self):
        decorator = enroute.broker.query(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual(Response("test"), wrapper(self.request))

    async def test_broker_query_async(self):
        decorator = enroute.broker.query(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual(Response("test"), await wrapper(self.request))

    def test_broker_query_decorators(self):
        decorator = enroute.broker.query(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual({BrokerQueryEnrouteDecorator("CreateTicket")}, wrapper.__decorators__)

    def test_broker_event(self):
        decorator = enroute.broker.event(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual(Response("test"), wrapper(self.request))

    async def test_broker_event_async(self):
        decorator = enroute.broker.event(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual(Response("test"), await wrapper(self.request))

    def test_broker_event_decorators(self):
        decorator = enroute.broker.event(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual({BrokerEventEnrouteDecorator("CreateTicket")}, wrapper.__decorators__)

    def test_multiple_decorator_kind_raises(self):
        event_decorator = enroute.broker.event(topics=["CreateTicket"])
        command_decorator = enroute.broker.command(topics=["CreateTicket"])
        with self.assertRaises(MinosMultipleEnrouteDecoratorKindsException):
            event_decorator(command_decorator(_fn))


class TestEnrouteDecoratorAnalyzer(unittest.IsolatedAsyncioTestCase):
    def test_decorated_str(self):
        analyzer = EnrouteDecoratorAnalyzer(classname(FakeDecorated))
        self.assertEqual(FakeDecorated, analyzer.decorated)

    def test_multiple_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_all()
        expected = {
            FakeDecorated.get_tickets: {
                BrokerQueryEnrouteDecorator(("GetTickets",)),
                RestQueryEnrouteDecorator("tickets/", "GET"),
            },
            FakeDecorated.create_ticket: {
                BrokerCommandEnrouteDecorator(("CreateTicket", "AddTicket")),
                RestCommandEnrouteDecorator("orders/", "GET"),
            },
            FakeDecorated.ticket_added: {BrokerEventEnrouteDecorator("TicketAdded")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_rest_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_rest_command_query()
        expected = {
            FakeDecorated.get_tickets: {RestQueryEnrouteDecorator("tickets/", "GET")},
            FakeDecorated.create_ticket: {RestCommandEnrouteDecorator("orders/", "GET")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_command_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_broker_command_query()
        expected = {
            FakeDecorated.get_tickets: {BrokerQueryEnrouteDecorator("GetTickets")},
            FakeDecorated.create_ticket: {BrokerCommandEnrouteDecorator(("CreateTicket", "AddTicket"))},
        }

        self.assertEqual(expected, observed)

    def test_get_only_event_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(FakeDecorated)

        observed = analyzer.get_broker_event()
        expected = {FakeDecorated.ticket_added: {BrokerEventEnrouteDecorator("TicketAdded")}}

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
