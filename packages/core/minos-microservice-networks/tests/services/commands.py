from minos.networks import (
    BrokerResponse,
    Request,
    Response,
    enroute,
)


class CommandService:
    @enroute.rest.command(path="/order", method="GET", foo="bar")
    def get_order_rest(self, request: Request) -> Response:
        return Response("get_order")

    @enroute.rest.command(path="/order", method="DELETE")
    def delete_order_rest(self, request: Request) -> Response:
        return Response("delete_order")

    @enroute.broker.command("GetOrder")
    def get_order_command(self, request: Request) -> Response:
        return BrokerResponse("get_order")

    @enroute.broker.command("AddOrder")
    def add_order(self, request: Request) -> Response:
        return BrokerResponse("add_order")

    @enroute.broker.command("DeleteOrder")
    def delete_order(self, request: Request) -> Response:
        return BrokerResponse("delete_order")

    @enroute.broker.command("UpdateOrder")
    def update_order(self, request: Request) -> Response:
        return BrokerResponse("update_order")

    @enroute.broker.event("TicketAdded")
    def ticket_added(self, request: Request) -> str:
        return "command_service_ticket_added"

    @enroute.periodic.event("@daily")
    def recompute_something(self, request: Request) -> None:
        """For testing purposes."""
