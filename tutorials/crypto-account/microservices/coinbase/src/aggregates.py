from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Coinbase(RootEntity):
    """Coinbase RootEntity class."""


class CoinbaseAggregate(Aggregate[Coinbase]):
    """CoinbaseAggregate class."""

    @staticmethod
    async def create() -> UUID:
        """Create a new instance."""
        root = await Coinbase.create()
        return root.uuid