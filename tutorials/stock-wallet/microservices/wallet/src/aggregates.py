from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    Entity,
    EntitySet,
    RootEntity,
)


class Ticker(Entity):
    ticker: str
    is_crypto: bool


# LTSfWDXx9N2zMRNPyy_r
class Wallet(RootEntity):
    """Wallet RootEntity class."""

    name: str
    tickers: EntitySet[Ticker]


class WalletAggregate(Aggregate[Wallet]):
    """WalletAggregate class."""

    @staticmethod
    async def create_wallet(ticker_name: str) -> UUID:
        """Create a new instance."""
        root = await Wallet.create(name=ticker_name, tickers=EntitySet())
        return root.uuid

    @staticmethod
    async def add_ticker(wallet_uudi: str, ticker_value: str) -> UUID:
        """Create a new instance."""
        wallet = await Wallet.get(wallet_uudi)
        ticker = Ticker(ticker=ticker_value, is_crypto=False)
        wallet.tickers.add(ticker)
        await wallet.save()
        return ticker.uuid
