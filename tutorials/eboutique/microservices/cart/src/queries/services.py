from dependency_injector.wiring import (
    Provide,
)
from src.queries.repository import (
    CartQueryRepository,
)

from minos.aggregate import (
    Event,
)
from minos.cqrs import (
    QueryService,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)


class CartQueryService(QueryService):
    """CartQueryService class."""

    repository: CartQueryRepository = Provide["cart_repository"]

    @enroute.rest.query("/cart/{uuid}", "GET")
    async def get_cart(self, request: Request) -> Response:
        """Get a Cart instance.

        :param request: A request instance..
        :return: A response exception.
        """
        params = await request.params()
        cart_obj = self.repository.get(params["uuid"])
        raise Response(cart_obj)

    @enroute.broker.event("CartCreated")
    async def cart_created(self, request: Request) -> None:
        """Handle the Cart creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        self.repository.add(event)
        print(event)

    @enroute.broker.event("CartUpdated")
    async def cart_updated(self, request: Request) -> None:
        """Handle the Cart update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("CartDeleted")
    async def cart_deleted(self, request: Request) -> None:
        """Handle the Cart deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)
