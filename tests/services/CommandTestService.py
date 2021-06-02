from minos.common import (
    Request,
    Response,
)
from minos.networks import (
    CommandResponse,
)


class CommandService(object):
    async def get_order(self, request: Request) -> Response:
        return CommandResponse("get_order")

    async def add_order(self, request: Request) -> Response:
        return CommandResponse("add_order")

    async def delete_order(self, request: Request) -> Response:
        return CommandResponse("delete_order")

    async def update_order(self, request: Request) -> Response:
        return CommandResponse("update_order")
