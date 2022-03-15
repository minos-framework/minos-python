from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


# LTSfWDXx9N2zMRNPyy_r
class Wallet(RootEntity):
    """Wallet RootEntity class."""
    ticker: str


class WalletAggregate(Aggregate[Wallet]):
    """WalletAggregate class."""

    @staticmethod
    async def create(ticker_data: str) -> UUID:
        """Create a new instance."""
        root = await Wallet.create(ticker = ticker_data)
        return root.uuid
