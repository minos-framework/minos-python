from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)

from ..aggregates import (
    ShippingAggregate,
)


class ShippingCommandService(CommandService):
    """ShippingCommandService class."""

    @enroute.rest.command("/shippings", "POST")
    @enroute.broker.command("CreateShipping")
    async def create_shipping(self, request: Request) -> Response:
        """Create a new ``Shipping`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            uuid = await ShippingAggregate.create()
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Shipping creation: {exc}")