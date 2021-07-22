from minos.common import (
    Request,
    Response,
)
from minos.networks import (
    CommandResponse,
    enroute,
)


class CommandService(object):
    @enroute.broker.command(topics=["GetOrder"])
    def get_order(self, request: Request) -> Response:
        return CommandResponse("get_order")

    @enroute.broker.command(topics=["AddOrder"])
    def add_order(self, request: Request) -> Response:
        return CommandResponse("add_order")

    @enroute.broker.command(topics=["DeleteOrder"])
    def delete_order(self, request: Request) -> Response:
        return CommandResponse("delete_order")

    @enroute.broker.command(topics=["UpdateOrder"])
    def update_order(self, request: Request) -> Response:
        return CommandResponse("update_order")
