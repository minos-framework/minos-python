"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.networks import (
    EnrouteDecoratorAnalyzer,
    enroute,
)
from minos.networks.handlers import (
    BrokerCommandEnroute,
    BrokerEventEnroute,
    BrokerQueryEnroute,
    RestCommandEnroute,
    RestQueryEnroute,
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
    def get_tickets(self):
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


class TestDecorators(unittest.IsolatedAsyncioTestCase):
    async def test_rest_query_base(self):
        func = enroute.rest.query(url="tickets/", method="GET")
        wrapper = func(_fn)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_rest_query_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(RestQueryExample)

        observed = analyzer.get_all()
        expected = {RestQueryExample.get_tickets: {RestQueryEnroute("tickets/", "GET")}}

        self.assertEqual(expected, observed)

    async def test_rest_command(self):
        analyzer = EnrouteDecoratorAnalyzer(RestCommandExample)

        observed = analyzer.get_all()
        expected = {RestCommandExample.get_tickets: {RestCommandEnroute("orders/", "GET")}}

        self.assertEqual(expected, observed)

    async def test_rest_dont_have_event(self):
        with self.assertRaises(Exception) as context:
            enroute.rest.event(topics=["CreateTicket"])

        self.assertTrue("type object 'RestEnroute' has no attribute 'event'" in str(context.exception))

    async def test_rest_command_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(RestCommandExample)

        observed = analyzer.get_all()
        expected = {RestCommandExample.get_tickets: {RestCommandEnroute("orders/", "GET")}}

        self.assertEqual(expected, observed)

    async def test_broker_query(self):
        func = enroute.broker.query(topics=["CreateTicket"])
        wrapper = func(_fn)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_query_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerQueryExample)

        observed = analyzer.get_all()
        expected = {BrokerQueryExample.get_tickets: {BrokerQueryEnroute("BrokerQuery")}}

        self.assertEqual(expected, observed)

    async def test_broker_command(self):
        func = enroute.broker.command(topics=["CreateTicket"])
        wrapper = func(_fn)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_command_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerCommandExample)

        observed = analyzer.get_all()
        expected = {BrokerCommandExample.get_tickets: {BrokerCommandEnroute("BrokerCommand")}}

        self.assertEqual(expected, observed)

    async def test_broker_event(self):
        func = enroute.broker.event(topics=["CreateTicket"])
        wrapper = func(_fn)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_event_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerEventExample)
        observed = analyzer.get_all()

        expected = {BrokerEventExample.get_tickets: {BrokerEventEnroute(["BrokerEvent"])}}
        self.assertEqual(expected, observed)

    async def test_multiple_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.get_all()
        expected = {
            MultipleDecoratorsExample.get_tickets: {
                BrokerQueryEnroute(("TicketBrokerEvent",)),
                RestQueryEnroute("tickets/", "GET"),
            },
            MultipleDecoratorsExample.create_ticket: {
                BrokerCommandEnroute(("CreateTicket", "AddTicket")),
                RestCommandEnroute("orders/", "GET"),
            },
            MultipleDecoratorsExample.ticket_added: {BrokerEventEnroute("TicketAdded")},
        }

        self.assertEqual(expected, observed)

    async def test_get_only_rest_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.rest()
        expected = {
            MultipleDecoratorsExample.get_tickets: {RestQueryEnroute("tickets/", "GET")},
            MultipleDecoratorsExample.create_ticket: {RestCommandEnroute("orders/", "GET")},
        }

        self.assertEqual(expected, observed)

    async def test_get_only_command_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.command()
        expected = {
            MultipleDecoratorsExample.get_tickets: {BrokerQueryEnroute("TicketBrokerEvent")},
            MultipleDecoratorsExample.create_ticket: {BrokerCommandEnroute(("CreateTicket", "AddTicket"))},
        }

        self.assertEqual(expected, observed)

    async def test_get_only_event_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)

        observed = analyzer.event()
        expected = {MultipleDecoratorsExample.ticket_added: {BrokerEventEnroute("TicketAdded")}}

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
