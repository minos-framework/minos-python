from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Shipping(RootEntity):
    """Shipping RootEntity class."""


class ShippingAggregate(Aggregate[Shipping]):
    """ShippingAggregate class."""
