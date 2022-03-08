from dependency_injector.wiring import (
    Provide,
)
from src.queries.models import (
    Cart,
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
        cart_obj: Cart = self.repository.get(params["uuid"])
        return Response(cart_obj)

    @enroute.rest.query("/cart/{uuid}/items", "GET")
    async def get_cart_items(self, request: Request) -> Response:
        """Get a Cart instance.

        :param request: A request instance..
        :return: A response exception.
        """
        params = await request.params()
        items_obj = self.repository.get_items_cart(params["uuid"])
        return Response(items_obj)

    @enroute.broker.event("CartCreated")
    async def cart_created(self, request: Request) -> None:
        """Handle the Cart creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        self.repository.add(event)

    @enroute.broker.event("CartUpdated")
    async def cart_updated(self, request: Request) -> None:
        """Handle the Cart update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()

        cart_uuid = event["uuid"]
        items = event.get_all()
        product = await items["products"][0]["product"].resolve()
        self.repository.add_item(cart_uuid=cart_uuid, item=items["products"][0], product=product)
