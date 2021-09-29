from minos.networks import (
    HandlerResponse,
    Request,
    Response,
    enroute,
)


class CommandService:
    @enroute.rest.command(url="/order", method="GET")
    def get_order_rest(self, request: Request) -> Response:
        return Response("get_order")

    @enroute.broker.command("GetOrder")
    def get_order_command(self, request: Request) -> Response:
        return HandlerResponse("get_order")

    @enroute.broker.command("AddOrder")
    def add_order(self, request: Request) -> Response:
        return HandlerResponse("add_order")

    @enroute.broker.command("DeleteOrder")
    def delete_order(self, request: Request) -> Response:
        return HandlerResponse("delete_order")

    @enroute.broker.command("UpdateOrder")
    def update_order(self, request: Request) -> Response:
        return HandlerResponse("update_order")

    @enroute.broker.event("TicketAdded")
    def ticket_added(self, request: Request) -> None:
        return "command_service_ticket_added"

    @enroute.periodic.event("@daily")
    def recompute_something(self, request: Request) -> None:
        """For testing purposes."""
