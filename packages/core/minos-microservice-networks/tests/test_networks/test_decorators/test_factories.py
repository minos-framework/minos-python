import unittest
from collections.abc import (
    Callable,
)

from minos.common import (
    classname,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    EnrouteFactory,
    InMemoryRequest,
    MinosRedefinedEnrouteDecoratorException,
    PeriodicEventEnrouteDecorator,
    Response,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
    enroute,
)
from tests.utils import (
    FakeService,
    fake_middleware,
)


class TestEnrouteFactory(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = InMemoryRequest("test")
        self.builder = EnrouteFactory(FakeService, middleware=fake_middleware)

    def test_classes(self):
        self.assertEqual((FakeService,), self.builder.classes)

    def test_classes_str(self):
        builder = EnrouteFactory(classname(FakeService))
        self.assertEqual((FakeService,), builder.classes)

    async def test_get_rest_command_query(self):
        handlers = self.builder.get_rest_command_query()
        self.assertEqual(3, len(handlers))

        expected = Response("_(Get Tickets: test)_")
        observed = await handlers[RestQueryEnrouteDecorator("tickets/", "GET")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("_Create Ticket_")
        observed = await handlers[RestCommandEnrouteDecorator("orders/", "GET")](self.request)
        self.assertEqual(expected, observed)

        expected = None
        observed = await handlers[RestCommandEnrouteDecorator("orders/", "DELETE")](self.request)
        self.assertEqual(expected, observed)

    async def test_get_broker_event(self):
        handlers = self.builder.get_broker_event()
        self.assertEqual(1, len(handlers))

        expected = Response("_Ticket Added: [test]_")
        observed = await handlers[BrokerEventEnrouteDecorator("TicketAdded")](self.request)
        self.assertEqual(expected, observed)

    async def test_get_periodic_event(self):
        handlers = self.builder.get_periodic_event()
        self.assertEqual(1, len(handlers))

        expected = {Response("_newsletter sent!_"), Response("_checked inactive users!_")}
        # noinspection PyTypeChecker
        observed = set(await handlers[PeriodicEventEnrouteDecorator("@daily")](self.request))
        self.assertEqual(expected, observed)

    async def test_get_broker_command_query_event(self):
        handlers = self.builder.get_broker_command_query_event()
        self.assertEqual(5, len(handlers))

        expected = Response("_Ticket Added: [test]_")
        observed = await handlers[BrokerEventEnrouteDecorator("TicketAdded")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("_(Get Tickets: test)_")
        observed = await handlers[BrokerQueryEnrouteDecorator("GetTickets")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("_Create Ticket_")
        observed = await handlers[BrokerCommandEnrouteDecorator("CreateTicket")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("_Create Ticket_")
        observed = await handlers[BrokerCommandEnrouteDecorator("AddTicket")](self.request)
        self.assertEqual(expected, observed)

        expected = None
        observed = await handlers[BrokerCommandEnrouteDecorator("DeleteTicket")](self.request)
        self.assertEqual(expected, observed)

    async def test_get_broker_command_query(self):
        handlers = self.builder.get_broker_command_query()
        self.assertEqual(4, len(handlers))

        expected = Response("_(Get Tickets: test)_")
        observed = await handlers[BrokerQueryEnrouteDecorator("GetTickets")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("_Create Ticket_")
        observed = await handlers[BrokerCommandEnrouteDecorator("CreateTicket")](self.request)
        self.assertEqual(expected, observed)

        expected = Response("_Create Ticket_")
        observed = await handlers[BrokerCommandEnrouteDecorator("AddTicket")](self.request)
        self.assertEqual(expected, observed)

        expected = None
        observed = await handlers[BrokerCommandEnrouteDecorator("DeleteTicket")](self.request)
        self.assertEqual(expected, observed)

    async def test_get_all(self):
        expected = {
            BrokerCommandEnrouteDecorator("DeleteTicket"),
            BrokerCommandEnrouteDecorator("AddTicket"),
            BrokerCommandEnrouteDecorator("CreateTicket"),
            BrokerEventEnrouteDecorator("TicketAdded"),
            BrokerQueryEnrouteDecorator("GetTickets"),
            RestCommandEnrouteDecorator("orders/", "DELETE"),
            RestCommandEnrouteDecorator("orders/", "GET"),
            RestQueryEnrouteDecorator("tickets/", "GET"),
            PeriodicEventEnrouteDecorator("@daily"),
        }

        observed = self.builder.get_all()
        self.assertEqual(expected, set(observed.keys()))
        self.assertTrue(all(isinstance(o, Callable) for o in observed.values()))

    def test_raises_duplicated_command(self):
        class _WrongService:
            @enroute.rest.command(path="orders/", method="GET")
            def _fn1(self, request):
                return Response("bar")

            @enroute.rest.command(path="orders/", method="GET")
            def _fn2(self, request):
                return Response("bar")

        builder = EnrouteFactory(_WrongService)
        with self.assertRaises(MinosRedefinedEnrouteDecoratorException):
            builder.get_rest_command_query()

    def test_raises_duplicated_command_query(self):
        class _WrongService:
            @enroute.rest.command(path="orders/", method="GET")
            def _fn1(self, request):
                return Response("bar")

            @enroute.rest.query(path="orders/", method="GET")
            def _fn2(self, request):
                return Response("bar")

        builder = EnrouteFactory(_WrongService)
        with self.assertRaises(MinosRedefinedEnrouteDecoratorException):
            builder.get_rest_command_query()


if __name__ == "__main__":
    unittest.main()
