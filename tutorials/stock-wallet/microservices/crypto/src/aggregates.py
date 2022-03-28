from uuid import (
    UUID,
)

from minos.aggregate import Aggregate, RootEntity


class Crypto(RootEntity):
    """Crypto RootEntity class."""

    ticker: str
    updated: str


class CryptoAggregate(Aggregate[Crypto]):
    """CryptoAggregate class."""

    @staticmethod
    async def add_crypto_to_stock(ticker: str, updated: str) -> UUID:
        """Create a new instance."""
        crypto = await Crypto.create(ticker=ticker, updated=updated)
        return crypto

    @staticmethod
    async def get_all_tickers():
        all_stocks = [
            {"uuid": stock.uuid, "ticker": stock.ticker, "updated": stock.updated} async for stock in Crypto.get_all()
        ]
        return all_stocks

    @staticmethod
    async def update_time_ticker(stock_uuid: str, datetime: str):
        stock = await Crypto.get(stock_uuid)
        stock.updated = datetime
        await stock.save()
