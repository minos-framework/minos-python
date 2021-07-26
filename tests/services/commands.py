from minos.networks import (
    CommandResponse,
    Request,
    Response,
    enroute,
)


class CommandService(object):
    @enroute.rest.command(url="/order", method="GET")
    def get_order_rest(self, request: Request) -> Response:
        return Response("get_order")

    @enroute.broker.command("GetOrder")
    def get_order_command(self, request: Request) -> Response:
        return CommandResponse("get_order")

    @enroute.broker.command("AddOrder")
    def add_order(self, request: Request) -> Response:
        return CommandResponse("add_order")

    @enroute.broker.command("DeleteOrder")
    def delete_order(self, request: Request) -> Response:
        return CommandResponse("delete_order")

    @enroute.broker.command("UpdateOrder")
    def update_order(self, request: Request) -> Response:
        return CommandResponse("update_order")
