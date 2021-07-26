from minos.networks import (
    enroute,
)


class DecoratedService(object):
    @enroute.rest.query(url="tickets/", method="GET")
    def get_tickets(self):
        return "tickets"

    @enroute.rest.command(url="orders/", method="GET")
    def get_orders(self):
        return "orders"

    @enroute.broker.query("BrokerQuery")
    def get_payments(self):
        return "tickets"

    @enroute.broker.command("BrokerCommand")
    def get_cart(self):
        return "tickets"

    @enroute.broker.event("BrokerEvent")
    def get_item(self):
        return "tickets"
