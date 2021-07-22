from minos.common import (
    Request,
    Response,
)
from minos.networks import (
    HttpResponse,
    enroute,
)


class RestService(object):
    @enroute.rest.query(url="/order", method="POST")
    async def add_order(self, request: Request) -> Response:
        return HttpResponse("Order added")

    @enroute.rest.query(url="/order", method="GET")
    async def get_order(self, request: Request) -> Response:
        return HttpResponse("Order get")
