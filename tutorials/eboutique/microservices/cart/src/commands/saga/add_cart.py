from src import (
    Cart,
    CartItem,
)

from minos.saga import Saga, SagaContext, SagaRequest, SagaResponse
from minos.common import (
    ModelType,
)
from src.aggregates import CartAggregate


def _raise_error():
    return ValueError("The Product uid does not exist")


ProductGet = ModelType.build("ProductGet", {"uid": str})


def _get_product(context: SagaContext):
    # check if the product exist
    return SagaRequest("GetProductById", ProductGet(context["product_uid"]))


async def _get_product_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    content = await response.content()
    return context


async def _add_item_to_cart(context: SagaContext):
    cart = context["cart_uid"]
    product = context["product_uid"]
    quantity = context["quantity"]
    cart_obj = await CartAggregate.addCartItem(cart, product, quantity)
    return SagaContext(cart=cart_obj["uuid"])


ADD_CART_ITEM = Saga().remote_step(_get_product).on_success(_get_product_success).commit(_add_item_to_cart)


async def _add_item_to_cart(context: SagaContext):
    cart = context["cart_uid"]
    product = context["product_uid"]
    quantity = context["quantity"]
    cart = await Cart.get(cart)
    cart_item = await CartItem(product=product, cart=cart, quantity=quantity)
    cart.items.add(cart_item)
    await cart.save()
    return SagaContext(cart=cart)


ADD_CART_ITEM = Saga().remote_step(_get_product).on_error(_raise_error).commit(_add_item_to_cart)
