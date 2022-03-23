from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Currency(RootEntity):
    """Currency RootEntity class."""


class CurrencyAggregate(Aggregate[Currency]):
    """CurrencyAggregate class."""

    @staticmethod
    async def create() -> UUID:
        """Create a new instance."""
        root = await Currency.create()
        return root.uuid
