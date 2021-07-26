from minos.common import (
    Event,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)


class CqrsService(object):
    @enroute.rest.query(url="/ticket", method="POST")
    def add_ticket(self, request: Request) -> Response:
        return Response("ticket_added")

    @enroute.broker.event("TicketAdded")
    def ticket_added(self, topic: str, event: Event):
        return "request_added"

    @enroute.broker.event("TicketDeleted")
    def ticket_deleted(self, topic: str, event: Event):
        return "ticket_deleted"
