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
    @enroute.rest.command(url="orders/", method="GET")
    def get_tickets(self):
        return "orders"

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
    @enroute.rest.command(url="orders/", method="GET")
    @enroute.broker.event(topics=["TicketBrokerEvent"])
    def get_tickets(self):
        return "tickets"

    @enroute.rest.query(url="orders/", method="POST")
    @enroute.rest.command(url="orders/", method="GET")
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
        analyzer = EnrouteDecoratorAnalyzer(RestQueryExample)
        result = analyzer.get_all()

        obj = result[RestQueryExample.get_tickets][0]

        self.assertEqual("GET", obj.method)
        self.assertEqual("tickets/", obj.url)
        self.assertEqual(RestQueryEnroute, type(obj))

    async def test_rest_command(self):
        analyzer = EnrouteDecoratorAnalyzer(RestCommandExample)
        result = analyzer.get_all()

        obj = result[RestCommandExample.get_tickets][0]

        self.assertEqual("GET", obj.method)
        self.assertEqual("orders/", obj.url)
        self.assertEqual(RestCommandEnroute, type(obj))

    async def test_rest_dont_have_event(self):
        with self.assertRaises(Exception) as context:
            enroute.rest.event(topics=["CreateTicket"])

        self.assertTrue("type object 'RestEnroute' has no attribute 'event'" in str(context.exception))

    async def test_rest_command_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(RestCommandExample)
        result = analyzer.get_all()

        obj = result[RestCommandExample.get_tickets][0]

        self.assertEqual("GET", obj.method)
        self.assertEqual("orders/", obj.url)
        self.assertEqual(RestCommandEnroute, type(obj))

    async def test_broker_query(self):
        func = enroute.broker.query(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_query_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerQueryExample)
        result = analyzer.get_all()

        obj = result[BrokerQueryExample.get_tickets][0]

        self.assertEqual(["BrokerQuery"], obj.topics)
        self.assertEqual(BrokerQueryEnroute, type(obj))

    async def test_broker_command(self):
        func = enroute.broker.command(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_command_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerCommandExample)
        result = analyzer.get_all()

        obj = result[BrokerCommandExample.get_tickets][0]

        self.assertEqual(["BrokerCommand"], obj.topics)
        self.assertEqual(BrokerCommandEnroute, type(obj))

    async def test_broker_event(self):
        func = enroute.broker.event(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_event_decorator(self):
        analyzer = EnrouteDecoratorAnalyzer(BrokerEventExample)
        result = analyzer.get_all()

        obj = result[BrokerEventExample.get_tickets][0]

        self.assertEqual(["BrokerEvent"], obj.topics)
        self.assertEqual(BrokerEventEnroute, type(obj))

    async def test_multiple_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)
        result = analyzer.get_all()

        self.assertEqual(3, len(result[MultipleDecoratorsExample.get_tickets]))
        self.assertEqual(5, len(result[MultipleDecoratorsExample.get_orders]))

    async def test_get_only_rest_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)
        result = analyzer.rest()

        self.assertEqual(2, len(result[MultipleDecoratorsExample.get_tickets]))
        self.assertEqual(2, len(result[MultipleDecoratorsExample.get_orders]))

    async def test_get_only_command_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)
        result = analyzer.command()

        self.assertEqual(2, len(result[MultipleDecoratorsExample.get_orders]))

    async def test_get_only_event_decorators(self):
        analyzer = EnrouteDecoratorAnalyzer(MultipleDecoratorsExample)
        result = analyzer.event()

        self.assertEqual(1, len(result[MultipleDecoratorsExample.get_tickets]))
        self.assertEqual(1, len(result[MultipleDecoratorsExample.get_orders]))


if __name__ == "__main__":
    unittest.main()
