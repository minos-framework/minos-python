from src.aggregates import (
    CartAggregate,
)

from minos.common import (
    ModelType,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaRequest,
    SagaResponse,
)


def _raise_error(context: SagaContext, response: SagaResponse) -> SagaContext:
    return ValueError("The Product uid does not exist")


ProductGet = ModelType.build("ProductGet", {"uid": str})


def _get_product(context: SagaContext):
    # check if the product exist
    return SagaRequest("GetProductById", ProductGet(uid=context["product_uid"]))


async def _get_product_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    content = await response.content()
    context["product"] = content
    return context


async def _add_item_to_cart(context: SagaContext):
    cart = context["cart_uid"]
    product = context["product_uid"]
    quantity = context["quantity"]
    cart_obj = await CartAggregate.addCartItem(cart, product, quantity)
    return SagaContext(cart=cart_obj)


ADD_CART_ITEM = (
    Saga().remote_step(_get_product).on_error(_raise_error).on_success(_get_product_success).commit(_add_item_to_cart)
)
