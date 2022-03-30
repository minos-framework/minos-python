import logging
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
    flag: str


class Wallet(RootEntity):
    """Wallet RootEntity class."""

    wallet_name: str
    tickers: EntitySet[Ticker]


logger = logging.getLogger(__name__)


class WalletAggregate(Aggregate[Wallet]):
    """WalletAggregate class."""

    @staticmethod
    async def create_wallet(ticker_name: str) -> UUID:
        """Create a new instance."""
        root = await Wallet.create(wallet_name=ticker_name, tickers=EntitySet())
        return root.uuid

    @staticmethod
    async def add_ticker(wallet_uudi: str, ticker_value: str, flag: str) -> UUID:
        """Create a new instance."""
        wallet = await Wallet.get(wallet_uudi)
        ticker = Ticker(ticker=ticker_value, flag=flag)
        logger.info("Added following information ticker {}, {}, {}".format(wallet_uudi, ticker_value, flag))
        wallet.tickers.add(ticker)
        await wallet.save()
        return ticker.uuid
