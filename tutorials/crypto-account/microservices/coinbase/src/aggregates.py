from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Coinbase(RootEntity):
    """Coinbase RootEntity class."""
    user: str
    api_key: str
    api_secret: str


class CoinbaseAggregate(Aggregate[Coinbase]):
    """CoinbaseAggregate class."""

    @staticmethod
    async def create(user: str, api_key: str, api_secret) -> UUID:
        """Create a new instance."""
        root = await Coinbase.create(user=user, api_key=api_key, api_secret=api_secret)
        return root.uuid
