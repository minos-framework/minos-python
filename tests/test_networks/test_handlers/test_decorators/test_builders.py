"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Response,
    classname,
)
from minos.networks import (
    EnrouteBuilder,
    MinosRedefinedEnrouteDecoratorException,
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


class TestEnrouteBuilder(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = FakeRequest("test")
        self.builder = EnrouteBuilder(FakeService)

    def test_instance_str(self):
        builder = EnrouteBuilder(classname(FakeService))
        self.assertEqual(FakeService, builder.decorated)

    async def test_get_rest_command_query(self):
        handlers = self.builder.get_rest_command_query()
        self.assertEqual(2, len(handlers))

        expected_response = Response("Get Tickets: test")
        observed_response = await handlers[RestQueryEnrouteDecorator("tickets/", "GET")](self.request)
        self.assertEqual(expected_response, observed_response)

        expected_response = Response("Create Ticket")
        observed_response = await handlers[RestCommandEnrouteDecorator("orders/", "GET")](self.request)
        self.assertEqual(expected_response, observed_response)

    async def test_get_broker_event(self):
        handlers = self.builder.get_broker_event()
        self.assertEqual(1, len(handlers))

        expected_response = Response("Ticket Added: test")
        observed_response = await handlers[BrokerEventEnrouteDecorator("TicketAdded")](self.request)
        self.assertEqual(expected_response, observed_response)

    async def test_get_broker_command_query(self):
        handlers = self.builder.get_broker_command_query()
        self.assertEqual(3, len(handlers))

        expected_response = Response("Get Tickets: test")
        observed_response = await handlers[BrokerQueryEnrouteDecorator("GetTickets")](self.request)
        self.assertEqual(expected_response, observed_response)

        expected_response = Response("Create Ticket")
        observed_response = await handlers[BrokerCommandEnrouteDecorator("CreateTicket")](self.request)
        self.assertEqual(expected_response, observed_response)

        expected_response = Response("Create Ticket")
        observed_response = await handlers[BrokerCommandEnrouteDecorator("AddTicket")](self.request)
        self.assertEqual(expected_response, observed_response)

    def test_raises(self):
        class _BadService:
            @enroute.rest.command(url="orders/", method="GET")
            def _fn1(self, request):
                return Response("bar")

            @enroute.rest.command(url="orders/", method="GET")
            def _fn2(self, request):
                return Response("bar")

        builder = EnrouteBuilder(_BadService)
        with self.assertRaises(MinosRedefinedEnrouteDecoratorException):
            builder.get_rest_command_query()


if __name__ == "__main__":
    unittest.main()
