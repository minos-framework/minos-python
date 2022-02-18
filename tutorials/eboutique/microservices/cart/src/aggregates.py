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
    title: str
    price: Ref[Price]


class CartItem(Entity):
    product: Ref[Product]
    quantity: int


class Cart(RootEntity):
    """Cart RootEntity class."""
    customer: str
    status: str
    items: Optional[EntitySet[CartItem]]


class CartAggregate(Aggregate[Cart]):
    """CartAggregate class."""

    @staticmethod
    async def createCart(data: {}) -> UUID:
        """Create a new Cart."""
        data['status'] = "open"
        cart = await Cart.create(customer=data['customer'], status=data['status'])
        return cart.uuid

    @staticmethod
    async def add_item(product: UUID, quantity: UUID, cart: UUID, user: str) -> bool:
        """add item product to Cart Entity."""
        car_object = await Cart.get(cart)

        return True
