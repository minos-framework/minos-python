from minos.common import (
    Aggregate,
    AggregateRef,
)


class CartItem(AggregateRef):
    """Aggregate ``Owner`` class for testing purposes."""

    name: str
    quantity: int


class Cart(Aggregate):
    """Aggregate ``Car`` class for testing purposes."""

    user: int
    items: list[CartItem]
