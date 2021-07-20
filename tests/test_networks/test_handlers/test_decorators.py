"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.networks import (
    EnrouteData,
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

        obj = result[next(iter(result))][0]

        self.assertEqual("GET", obj.method)
        self.assertEqual("tickets/", obj.url)
        self.assertEqual(RestQueryEnroute, type(obj))

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

        obj = result[next(iter(result))][0]

        self.assertEqual(["RestCommand"], obj.topics)
        self.assertEqual(RestCommandEnroute, type(obj))

    async def test_broker_query(self):
        func = enroute.broker.query(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_query_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(BrokerQueryExample)

        obj = result[next(iter(result))][0]

        self.assertEqual(["BrokerQuery"], obj.topics)
        self.assertEqual(BrokerQueryEnroute, type(obj))

    async def test_broker_command(self):
        func = enroute.broker.command(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_command_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(BrokerCommandExample)

        obj = result[next(iter(result))][0]

        self.assertEqual(["BrokerCommand"], obj.topics)
        self.assertEqual(BrokerCommandEnroute, type(obj))

    async def test_broker_event(self):
        func = enroute.broker.event(topics=["CreateTicket"])
        wrapper = func(decorated_func)
        result = wrapper()
        self.assertEqual("test", result)

    async def test_broker_event_decorator(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(BrokerEventExample)

        obj = result[next(iter(result))][0]

        self.assertEqual(["BrokerEvent"], obj.topics)
        self.assertEqual(BrokerEventEnroute, type(obj))

    async def test_multiple_decorators(self):
        result = EnrouteDecoratorAnalyzer.find_in_class(MultipleDecoratorsExample)

        res_iter = iter(result)

        self.assertEqual(3, len(result[next(res_iter)]))
        self.assertEqual(5, len(result[next(res_iter)]))

    async def test_get_only_rest_decorators(self):
        e = EnrouteData(MultipleDecoratorsExample)
        result = e.rest()

        res_iter = iter(result)

        self.assertEqual(1, len(result[next(res_iter)]))
        self.assertEqual(1, len(result[next(res_iter)]))

    async def test_get_only_command_decorators(self):
        e = EnrouteData(MultipleDecoratorsExample)
        result = e.command()

        res_iter = iter(result)

        self.assertEqual(1, len(result[next(res_iter)]))
        self.assertEqual(2, len(result[next(res_iter)]))

    async def test_get_only_event_decorators(self):
        e = EnrouteData(MultipleDecoratorsExample)
        result = e.event()

        res_iter = iter(result)

        self.assertEqual(1, len(result[next(res_iter)]))
        self.assertEqual(1, len(result[next(res_iter)]))


if __name__ == "__main__":
    unittest.main()
