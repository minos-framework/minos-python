import unittest

from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    GraphqlQueryEnrouteDecorator,
    PeriodicEventEnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
    enroute,
)


class TestEnroute(unittest.IsolatedAsyncioTestCase):
    def test_rest_command(self):
        decorator = enroute.rest.command(path="tickets/", method="GET")
        self.assertEqual(RestCommandEnrouteDecorator("tickets/", "GET"), decorator)

    def test_rest_query(self):
        decorator = enroute.rest.query(path="tickets/", method="GET")
        self.assertEqual(RestQueryEnrouteDecorator("tickets/", "GET"), decorator)

    def test_rest_event_raises(self):
        with self.assertRaises(AttributeError):
            enroute.rest.event("CreateTicket")

    def test_broker_command_decorators(self):
        decorator = enroute.broker.command("CreateTicket")
        self.assertEqual(BrokerCommandEnrouteDecorator("CreateTicket"), decorator)

    def test_broker_query_decorators(self):
        decorator = enroute.broker.query("CreateTicket")
        self.assertEqual(BrokerQueryEnrouteDecorator("CreateTicket"), decorator)

    def test_broker_event_decorators(self):
        decorator = enroute.broker.event("CreateTicket")
        self.assertEqual(BrokerEventEnrouteDecorator("CreateTicket"), decorator)

    def test_periodic_command_decorators(self):
        decorator = enroute.periodic.event("0 */2 * * *")
        self.assertEqual(PeriodicEventEnrouteDecorator("0 */2 * * *"), decorator)

    def test_not_found(self):
        with self.assertRaises(AttributeError):
            enroute.foo.command()

    def test_register(self):
        enroute._register_sub_enroute("foo", "bar")
        self.assertEqual("bar", enroute.foo)
        enroute._unregister_sub_enroute("foo")
        with self.assertRaises(AttributeError):
            enroute.foo


if __name__ == "__main__":
    unittest.main()
