from minos.aggregate import (
    Event,
)
from minos.cqrs import (
    QueryService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)


class ShippingQueryService(QueryService):
    """ShippingQueryService class."""

    @enroute.rest.query("/shippings", "GET")
    async def get_shipping(self, request: Request) -> Response:
        """Get a Shipping instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("ShippingCreated")
    async def shipping_created(self, request: Request) -> None:
        """Handle the Shipping creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("ShippingUpdated")
    async def shipping_updated(self, request: Request) -> None:
        """Handle the Shipping update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("ShippingDeleted")
    async def shipping_deleted(self, request: Request) -> None:
        """Handle the Shipping deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)