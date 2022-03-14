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


class CheckoutQueryService(QueryService):
    """CheckoutQueryService class."""

    @enroute.rest.query("/checkouts", "GET")
    async def get_checkout(self, request: Request) -> Response:
        """Get a Checkout instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("CheckoutCreated")
    async def checkout_created(self, request: Request) -> None:
        """Handle the Checkout creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("CheckoutUpdated")
    async def checkout_updated(self, request: Request) -> None:
        """Handle the Checkout update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("CheckoutDeleted")
    async def checkout_deleted(self, request: Request) -> None:
        """Handle the Checkout deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)