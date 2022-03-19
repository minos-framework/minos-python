from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Stocks(RootEntity):
    """Stocks RootEntity class."""


class StocksAggregate(Aggregate[Stocks]):
    """StocksAggregate class."""

    @staticmethod
    async def create() -> UUID:
        """Create a new instance."""
        stocks = await Stocks.create()
        return stocks
