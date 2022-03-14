from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Wallet(RootEntity):
    """Wallet RootEntity class."""


class WalletAggregate(Aggregate[Wallet]):
    """WalletAggregate class."""

    @staticmethod
    async def create() -> UUID:
        """Create a new instance."""
        root = await Wallet.create()
        return root.uuid
