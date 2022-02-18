from minos.saga import (
    Saga,
    SagaContext,
    SagaRequest
)

from src import Cart, CartItem


def _raise_error():
    return ValueError("The Product uid does not exist")


def _get_product(context: SagaContext):
    # check if the product exist
    return SagaRequest("GetProductById", {'uid': context['product_uid']})


async def _add_item_to_cart(context: SagaContext):
    cart = context['cart_uid']
    product = context['product_uid']
    quantity = context['quantity']
    cart = await Cart.get(cart)
    cart_item = await CartItem(product=product, cart=cart, quantity=quantity)
    cart.items.add(cart_item)
    await cart.save()
    return SagaContext(cart=cart)


ADD_CART_ITEM = (
    Saga().remote_step(_get_product).on_error(_raise_error).commit(_add_item_to_cart)
)
