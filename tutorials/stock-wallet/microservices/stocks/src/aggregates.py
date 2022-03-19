from uuid import (
    UUID,
)

from minos.aggregate import Aggregate, RootEntity, Entity


class Stocks(RootEntity):
    """Stocks RootEntity class."""

    ticker: str
    updated: str


class StocksAggregate(Aggregate[Stocks]):
    """StocksAggregate class."""

    @staticmethod
    async def add_ticker_to_stock(ticker: str) -> UUID:
        """Create a new instance."""

        stocks = await Stocks.create(ticker=ticker, updated="Never")
        return stocks

    @staticmethod
    async def update_time_ticker(stock_uuid: str, datetime: str):
        stock = Stocks.get(stock_uuid)
        stock.updated = datetime
        await stock.save()

    @staticmethod
    async def get_all_tickers():
        all_stocks = Stocks.get_all()
        tickers = []
        for stock in all_stocks:
            tickers.append({"uuid": stock.uuid, "ticker": stock.ticker, "updated": stock.updated})
        return tickers
