from typing import Optional
from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
    Entity,
    ExternalEntity,
    EntitySet,
    Ref
)


class Price(ExternalEntity):
    currency: str
    units: int


class Product(ExternalEntity):
    name: str
    price: Ref[Price]


class CartItem(Entity):
    product: Ref[Product]
    quantity: int


class Cart(RootEntity):
    """Cart RootEntity class."""
    user: str
    status: str
    items: Optional[EntitySet[CartItem]]


class CartAggregate(Aggregate[Cart]):
    """CartAggregate class."""

    @staticmethod
    async def createCart(cart) -> UUID:
        """Create a new Cart."""
        cart['status'] = "open"
        cart = await Cart.create(**cart)
        return cart.uuid

    @staticmethod
    async def add_item(product: UUID, quantity: UUID, cart: UUID, user: str) -> bool:
        """add item product to Cart Entity."""
        cart = await Cart.get(cart)

        return True
