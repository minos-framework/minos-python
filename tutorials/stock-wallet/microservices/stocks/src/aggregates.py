from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    EntitySet,
    RootEntity,
)

class Stocks(RootEntity):
    """Stocks RootEntity class."""

    ticker: str
    updated: str


class StocksAggregate(Aggregate[Stocks]):
    """StocksAggregate class."""

    @staticmethod
    async def add_ticker_to_stock(ticker: str, updated: str) -> UUID:
        """Create a new instance."""
        stocks = await Stocks.create(ticker=ticker, updated=updated)
        return stocks

    @staticmethod
    async def update_time_ticker(stock_uuid: str, datetime: str):
        stock = await Stocks.get(stock_uuid)
        stock.updated = datetime
        await stock.save()

    @staticmethod
    async def get_all_tickers():
        all_stocks = [
            {"uuid": stock.uuid, "ticker": stock.ticker, "updated": stock.updated} async for stock in Stocks.get_all()
        ]
        return all_stocks
