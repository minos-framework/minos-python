from minos.common import (
    Event,
)

from minos.networks import enroute


class CqrsService(object):
    @enroute.broker.event(topics=["TicketAdded"])
    def ticket_added(self, topic: str, event: Event):
        return "request_added"

    @enroute.broker.event(topics=["TicketDeleted"])
    def ticket_deleted(self, topic: str, event: Event):
        return "ticket_deleted"
