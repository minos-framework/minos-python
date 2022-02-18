from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)
from minos.saga import (
    SagaContext,
)
from .saga.add_cart import ADD_CART_ITEM
from ..aggregates import (
    CartAggregate,
)


class CartCommandService(CommandService):
    """CartCommandService class."""

    @enroute.rest.command("/cart", "POST")
    async def create_cart(self, request: Request) -> Response:
        """Create a new ``Cart`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()
            uuid = await CartAggregate.createCart(content)
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Cart creation:{content} {exc}")
        return Response({"uuid": uuid})

    @enroute.rest.command("/cart/{uuid}/item", "POST")
    async def create_cart_item(self, request: Request) -> Response:
        """Create a new ``Cart`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        # try:
        #     data = await request.content()
        #     params = await request.params()
        #
        #     saga_execution = await self.saga_manager.run(
        #         ADD_CART_ITEM, context=SagaContext(cart_uid=params['uuid'], product_uid=data['product'],
        #                                            quantity=data['quantity'])
        #     )
        #     return Response({"saga_uid": saga_execution.uuid})
        # except Exception as exc:
        #     raise ResponseException(f"An error occurred during Cart creation: {exc}")

        data = await request.content()
        params = await request.params()

        saga_execution = await self.saga_manager.run(
            ADD_CART_ITEM, context=SagaContext(cart_uid=params['uuid'], product_uid=data['product'],
                                               quantity=data['quantity'])
        )
        return Response({"saga_uid": saga_execution.uuid})
