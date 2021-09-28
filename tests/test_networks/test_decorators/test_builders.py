import unittest

from minos.common import (
    classname,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    EnrouteBuilder,
    MinosRedefinedEnrouteDecoratorException,
    Response,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
    enroute,
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
        self.assertEqual(3, len(handlers))

        expected = Response("Get Tickets: test")
        observed = await handlers[RestQueryEnrouteDecorator("tickets/", "GET")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("Create Ticket")
        observed = await handlers[RestCommandEnrouteDecorator("orders/", "GET")](self.request)
        self.assertEqual(expected, observed)

        expected = None
        observed = await handlers[RestCommandEnrouteDecorator("orders/", "DELETE")](self.request)
        self.assertEqual(expected, observed)

    async def test_get_broker_event(self):
        handlers = self.builder.get_broker_event()
        self.assertEqual(1, len(handlers))

        expected = Response("Ticket Added: [test]")
        observed = await handlers[BrokerEventEnrouteDecorator("TicketAdded")](self.request)
        self.assertEqual(expected, observed)

    async def test_get_broker_command_query(self):
        handlers = self.builder.get_broker_command_query()
        self.assertEqual(4, len(handlers))

        expected = Response("Get Tickets: test")
        observed = await handlers[BrokerQueryEnrouteDecorator("GetTickets")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("Create Ticket")
        observed = await handlers[BrokerCommandEnrouteDecorator("CreateTicket")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("Create Ticket")
        observed = await handlers[BrokerCommandEnrouteDecorator("AddTicket")](self.request)
        self.assertEqual(expected, observed)

        expected = None
        observed = await handlers[BrokerCommandEnrouteDecorator("DeleteTicket")](self.request)
        self.assertEqual(expected, observed)

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
