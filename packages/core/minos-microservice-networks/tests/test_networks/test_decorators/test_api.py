import unittest

from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    PeriodicEventEnrouteDecorator,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
    GraphqlQueryEnrouteDecorator,
    enroute,
)


class TestEnroute(unittest.IsolatedAsyncioTestCase):
    def test_rest_command(self):
        decorator = enroute.rest.command(url="tickets/", method="GET")
        self.assertEqual(RestCommandEnrouteDecorator("tickets/", "GET"), decorator)

    def test_rest_query(self):
        decorator = enroute.rest.query(url="tickets/", method="GET")
        self.assertEqual(RestQueryEnrouteDecorator("tickets/", "GET"), decorator)

    def test_graphql(self):
        decorator = enroute.graphql(url="graphql/", method="POST")
        self.assertEqual(GraphqlQueryEnrouteDecorator("graphql/", "POST"), decorator)

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


if __name__ == "__main__":
    unittest.main()
