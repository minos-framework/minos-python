from minos.common import (
    Request,
    Response,
)
from minos.networks import (
    HttpResponse,
)


class RestService(object):
    async def add_order(self, request: Request) -> Response:
        return HttpResponse("Order added")

    async def get_order(self, request: Request) -> Response:
        return HttpResponse("Order get")
