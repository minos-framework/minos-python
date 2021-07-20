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
    @enroute.rest.query(url="tickets/", method="GET")
    def get_tickets(self):
        return "tickets"

    def bar(self):
        return "bar"


class RestCommandExample:
    @enroute.rest.command(topics=["RestCommand"])
    def get_tickets(self):
        return "tickets"

    def bar(self):
        return "bar"


class BrokerQueryExample:
    @enroute.broker.query(topics=["BrokerQuery"])
    def get_tickets(self):
        return "tickets"

    def bar(self):
        return "bar"


class BrokerCommandExample:
    @enroute.broker.command(topics=["BrokerCommand"])
    def get_tickets(self):
        return "tickets"

    def bar(self):
        return "bar"


class BrokerEventExample:
    @enroute.broker.event(topics=["BrokerEvent"])
    def get_tickets(self):
        return "tickets"

    def bar(self):
        return "bar"


class MultipleDecoratorsExample:
    @enroute.rest.query(url="tickets/", method="GET")
    @enroute.rest.command(topics=["TicketRestCommand", "TicketRestCommand2"])
    @enroute.broker.event(topics=["TicketBrokerEvent"])
    def get_tickets(self):
        return "tickets"

    @enroute.rest.query(url="orders/", method="POST")
    @enroute.rest.command(topics=["OrderRestCommand", "OrderRestCommand2"])
    @enroute.broker.query(topics=["OrderBrokerQuery"])
    @enroute.broker.command(topics=["OrderBrokerCommand", "OrderBrokerCommand2"])
    @enroute.broker.event(topics=["OrderBrokerEvent"])
    def get_orders(self):
        return "orders"

    def bar(self):
        return "bar"


def decorated_func():
    return "test"


class TestDecorators(unittest.IsolatedAsyncioTestCase):
    async def test_rest_query_base(self):
        func = enroute.rest.query(url="tickets/", method="GET")
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_rest_query_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(RestQueryExample)

        self.assertEqual("GET", result["get_tickets"][0].method)
        self.assertEqual("tickets/", result["get_tickets"][0].url)
        self.assertEqual(RestQueryEnroute, type(result["get_tickets"][0]))

    async def test_rest_command(self):
        func = enroute.rest.command(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_rest_dont_have_event(self):
        with self.assertRaises(Exception) as context:
            enroute.rest.event(topics=["CreateTicket"])

        self.assertTrue("type object 'RestEnroute' has no attribute 'event'" in str(context.exception))

    async def test_rest_command_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(RestCommandExample)

        self.assertEqual(["RestCommand"], result["get_tickets"][0].topics)
        self.assertEqual(RestCommandEnroute, type(result["get_tickets"][0]))

    async def test_broker_query(self):
        func = enroute.broker.query(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_query_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(BrokerQueryExample)

        self.assertEqual(["BrokerQuery"], result["get_tickets"][0].topics)
        self.assertEqual(BrokerQueryEnroute, type(result["get_tickets"][0]))

    async def test_broker_command(self):
        func = enroute.broker.command(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_command_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(BrokerCommandExample)

        self.assertEqual(["BrokerCommand"], result["get_tickets"][0].topics)
        self.assertEqual(BrokerCommandEnroute, type(result["get_tickets"][0]))

    async def test_broker_event(self):
        func = enroute.broker.event(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_event_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(BrokerEventExample)

        self.assertEqual(["BrokerEvent"], result["get_tickets"][0].topics)
        self.assertEqual(BrokerEventEnroute, type(result["get_tickets"][0]))

    async def test_multiple_decorators(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(MultipleDecoratorsExample)

        self.assertEqual(3, len(result["get_tickets"]))
        self.assertEqual(5, len(result["get_orders"]))


if __name__ == "__main__":
    unittest.main()
