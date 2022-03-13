from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Shipping(RootEntity):
    """Shipping RootEntity class."""


class ShippingAggregate(Aggregate[Shipping]):
    """ShippingAggregate class."""

    @staticmethod
    async def create() -> UUID:
        """Create a new instance."""
        root = await Shipping.create()
        return root.uuid