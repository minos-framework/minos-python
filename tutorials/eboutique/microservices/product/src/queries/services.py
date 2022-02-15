from dependency_injector.wiring import Provide
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

from src.queries.repository import ProductQueryRepository


class ProductQueryService(QueryService):
    """ProductQueryService class."""

    repository: ProductQueryRepository = Provide["product_repository"]

    @enroute.rest.query("/products", "GET")
    async def get_products(self, request: Request) -> Response:
        """Get a Product instance.

        :param request: A request instance..
        :return: A response exception.
        """
        return Response(self.repository.get_all())

    @enroute.rest.query("/product/{uuid}", "GET")
    async def get_product(self, request: Request) -> Response:
        """Get a Product instance.

        :param request: A request instance..
        :return: A response exception.
        """
        params = await request.params()
        product = self.repository.get(params["uuid"])
        return Response(product)

    @enroute.broker.event("ProductCreated")
    async def product_created(self, request: Request) -> None:
        """Handle the Product creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        self.repository.add(event)

    @enroute.broker.event("ProductUpdated")
    async def product_updated(self, request: Request) -> None:
        """Handle the Product update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("ProductDeleted")
    async def product_deleted(self, request: Request) -> None:
        """Handle the Product deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)
