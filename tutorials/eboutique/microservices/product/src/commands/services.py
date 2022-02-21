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
    ProductAggregate,
)


class ProductCommandService(CommandService):
    """ProductCommandService class."""

    @enroute.broker.command("GetProductById")
    async def get_product_by_id(self, request: Request) -> Response:
        """Create a new ``Product`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()  # get the request payload
            product = await ProductAggregate.getProduct(content['uid'])
            return Response(product.uuid)
        except Exception as exc:
            raise ResponseException(f"An error occurred during the Query process: {exc}")

    @enroute.rest.command("/product", "POST")
    @enroute.broker.command("CreateProduct")
    async def create_product(self, request: Request) -> Response:
        """Create a new ``Product`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()  # get the request payload
            uuid = await ProductAggregate.createProduct(content)
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during the Product creation: {exc}")

    @enroute.rest.command("/product/{uuid}", "DELETE")
    @enroute.broker.command("DeleteProduct")
    async def delete_product(self, request: Request) -> Response:
        """Delete a  ``Product`` .

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            params = request.params()  # get the url params [uuid]
            uuid = params["uuid"]
            uuid = await ProductAggregate.deleteProduct(uuid)
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Product delete: {exc}")
