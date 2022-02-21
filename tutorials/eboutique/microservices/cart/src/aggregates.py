from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    Entity,
    EntitySet,
    ExternalEntity,
    Ref,
    RootEntity,
)


class Price(ExternalEntity):
    currency: str
    units: int


class Product(ExternalEntity):
    title: str
    price: Ref[Price]


class CartItem(Entity):
    product: Ref[Product]
    quantity: int


class Cart(RootEntity):
    user: str
    """Cart RootEntity class."""
    status: str
    products: EntitySet[CartItem]


class CartAggregate(Aggregate[Cart]):
    """CartAggregate class."""

    @staticmethod
    async def createCart(data: {}) -> UUID:
        """Create a new Cart."""
        data["status"] = "open"
        cart = await Cart.create(user=data["user"], status=data["status"], products=EntitySet())
        return cart.uuid

    @staticmethod
    async def addCartItem(cart: str, product: str, quantity) -> UUID:
        """Create a new Cart."""
        cart = await Cart.get(cart)
        cart_item = CartItem(product=product, quantity=quantity)
        cart.products.add(cart_item)
        await cart.save()
        return cart
