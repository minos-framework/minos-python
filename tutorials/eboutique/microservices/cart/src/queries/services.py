import logging

from dependency_injector.wiring import (
    Provide,
)

from src.queries.models import Cart
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

logger = logging.getLogger(__name__)


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
        return Response({'uuid': cart_obj.uuid, 'user': cart_obj.user, 'status': cart_obj.status})

    @enroute.rest.query("/cart/{uuid}/items", "GET")
    async def get_cart_items(self, request: Request) -> Response:
        """Get a Cart instance.

        :param request: A request instance..
        :return: A response exception.
        """
        params = await request.params()
        cart_obj = self.repository.get_items_cart(params["uuid"])
        logger.warning(cart_obj)
        return Response(cart_obj)

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
        cart_uuid = event['uuid']
        items = event.get_all()
        self.repository.add_item(cart_uuid=cart_uuid, product=items['products'][0]['product'],
                                 item=items['products'][0])
