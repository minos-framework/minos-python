from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Checkout(RootEntity):
    """Checkout RootEntity class."""


class CheckoutAggregate(Aggregate[Checkout]):
    """CheckoutAggregate class."""

    @staticmethod
    async def create() -> UUID:
        """Create a new instance."""
        root = await Checkout.create()
        return root.uuid
