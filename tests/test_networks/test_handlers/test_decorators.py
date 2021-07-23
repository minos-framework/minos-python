"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
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


class RestQueryExample:
    """For testing purposes."""

    @enroute.rest.query(url="tickets/", method="GET")
    def get_tickets(self):
        """For testing purposes."""
        return "tickets"


class RestCommandExample:
    """For testing purposes."""

    @enroute.rest.command(url="orders/", method="GET")
    def get_tickets(self):
        """For testing purposes."""
        return "orders"


class BrokerQueryExample:
    """For testing purposes."""

    @enroute.broker.query(topics=["BrokerQuery"])
    def get_tickets(self):
        """For testing purposes."""
        return "tickets"


class BrokerCommandExample:
    """For testing purposes."""

    @enroute.broker.command(topics=["BrokerCommand"])
    def get_tickets(self):
        """For testing purposes."""
        return "tickets"


class BrokerEventExample:
    """For testing purposes."""

    @enroute.broker.event(topics=["BrokerEvent"])
    def get_tickets(self):
        """For testing purposes."""
        return "tickets"


class MultipleDecoratorsExample:
    """For testing purposes."""

    @enroute.rest.command(url="orders/", method="GET")
    @enroute.broker.command(topics=["CreateTicket", "AddTicket"])
    def create_ticket(self):
        """For testing purposes."""
        return "orders"

    @enroute.rest.query(url="tickets/", method="GET")
    @enroute.broker.query(topics=["TicketBrokerEvent"])
    async def get_tickets(self):
        """For testing purposes."""
        return "tickets"

    @enroute.broker.event(topics=["TicketAdded"])
    def ticket_added(self):
        """For testing purposes."""
        return "orders"

    # noinspection PyMethodMayBeStatic
    def bar(self):
        """For testing purposes."""
        return "bar"


def _fn():
    """For testing purposes."""
    return "test"


async def _async_fn():
    """For testing purposes."""
    return "test"


class TestEnroute(unittest.IsolatedAsyncioTestCase):
    def test_rest_command(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        wrapper = decorator(_fn)
        self.assertEqual("test", wrapper())

    async def test_rest_command_async(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        wrapper = decorator(_async_fn)
        self.assertEqual("test", await wrapper())

    def test_rest_command_decorators(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        wrapper = decorator(_fn)
        self.assertEqual({RestCommandEnrouteDecorator("tickets/", "GET")}, wrapper.__decorators__)

    def test_rest_query(self):
        decorator = enroute.rest.query(url="tickets/", method="GET")
        wrapper = decorator(_fn)
        self.assertEqual("test", wrapper())

    async def test_rest_query_async(self):
        decorator = enroute.rest.query(url="tickets/", method="GET")
        wrapper = decorator(_async_fn)
        self.assertEqual("test", await wrapper())

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
        self.assertEqual("test", wrapper())

    async def test_broker_command_async(self):
        decorator = enroute.broker.command(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual("test", await wrapper())

    def test_broker_command_decorators(self):
        decorator = enroute.broker.command(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual({BrokerCommandEnrouteDecorator("CreateTicket")}, wrapper.__decorators__)

    def test_broker_query(self):
        decorator = enroute.broker.query(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual("test", wrapper())

    async def test_broker_query_async(self):
        decorator = enroute.broker.query(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual("test", await wrapper())

    def test_broker_query_decorators(self):
        decorator = enroute.broker.query(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual({BrokerQueryEnrouteDecorator("CreateTicket")}, wrapper.__decorators__)

    def test_broker_event(self):
        decorator = enroute.broker.event(topics=["CreateTicket"])
        wrapper = decorator(_fn)
        self.assertEqual("test", wrapper())

    async def test_broker_event_async(self):
        decorator = enroute.broker.event(topics=["CreateTicket"])
        wrapper = decorator(_async_fn)
        self.assertEqual("test", await wrapper())

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
        analyzer = EnrouteDecoratorAnalyzer(classname(MultipleDecoratorsExample))
        self.assertEqual(MultipleDecoratorsExample, analyzer.decorated)

    def test_rest_query_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(RestQueryExample)

        observed = analyzer.get_all()
        expected = {RestQueryExample.get_tickets: {RestQueryEnrouteDecorator("tickets/", "GET")}}

        self.assertEqual(expected, observed)

    def test_rest_command(self):
        analyzer = EnrouteDecoratorAnalyzer(RestCommandExample)

        observed = analyzer.get_all()
        expected = {RestCommandExample.get_tickets: {RestCommandEnrouteDecorator("orders/", "GET")}}

        self.assertEqual(expected, observed)

    def test_rest_command_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(RestCommandExample)

        observed = analyzer.get_all()
        expected = {RestCommandExample.get_tickets: {RestCommandEnrouteDecorator("orders/", "GET")}}

        self.assertEqual(expected, observed)

    def test_broker_query_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerQueryExample)

        observed = analyzer.get_all()
        expected = {BrokerQueryExample.get_tickets: {BrokerQueryEnrouteDecorator("BrokerQuery")}}

        self.assertEqual(expected, observed)

    def test_broker_command_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerCommandExample)

        observed = analyzer.get_all()
        expected = {BrokerCommandExample.get_tickets: {BrokerCommandEnrouteDecorator("BrokerCommand")}}

        self.assertEqual(expected, observed)

    def test_broker_event_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerEventExample)
        observed = analyzer.get_all()

        expected = {BrokerEventExample.get_tickets: {BrokerEventEnrouteDecorator(["BrokerEvent"])}}
        self.assertEqual(expected, observed)

    def test_multiple_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.get_all()
        expected = {
            MultipleDecoratorsExample.get_tickets: {
                BrokerQueryEnrouteDecorator(("TicketBrokerEvent",)),
                RestQueryEnrouteDecorator("tickets/", "GET"),
            },
            MultipleDecoratorsExample.create_ticket: {
                BrokerCommandEnrouteDecorator(("CreateTicket", "AddTicket")),
                RestCommandEnrouteDecorator("orders/", "GET"),
            },
            MultipleDecoratorsExample.ticket_added: {BrokerEventEnrouteDecorator("TicketAdded")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_rest_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.get_rest_command_query()
        expected = {
            MultipleDecoratorsExample.get_tickets: {RestQueryEnrouteDecorator("tickets/", "GET")},
            MultipleDecoratorsExample.create_ticket: {RestCommandEnrouteDecorator("orders/", "GET")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_command_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.get_broker_command_query()
        expected = {
            MultipleDecoratorsExample.get_tickets: {BrokerQueryEnrouteDecorator("TicketBrokerEvent")},
            MultipleDecoratorsExample.create_ticket: {BrokerCommandEnrouteDecorator(("CreateTicket", "AddTicket"))},
        }

        self.assertEqual(expected, observed)

    def test_get_only_event_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.get_broker_event()
        expected = {MultipleDecoratorsExample.ticket_added: {BrokerEventEnrouteDecorator("TicketAdded")}}

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
