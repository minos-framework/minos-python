from minos.common import (
    Event,
    Request,
    Response,
)
from minos.networks import (
    enroute,
)


class CqrsService(object):
    @enroute.rest.query(url="/ticket", method="POST")
    def add_ticket(self, request: Request) -> Response:
        return Response("ticket_added")

    @enroute.broker.event(topics=["TicketAdded"])
    def ticket_added(self, topic: str, event: Event):
        return "request_added"

    @enroute.broker.event(topics=["TicketDeleted"])
    def ticket_deleted(self, topic: str, event: Event):
        return "ticket_deleted"
