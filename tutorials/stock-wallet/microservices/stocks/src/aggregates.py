from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    Entity,
    EntitySet,
    RootEntity,
)


class Quotes(Entity):
    close_value: float
    ticker_name: str
    volume: int
    when: str


class Stocks(RootEntity):
    """Stocks RootEntity class."""

    ticker: str
    updated: str
    quotes: EntitySet[Quotes]


class StocksAggregate(Aggregate[Stocks]):
    """StocksAggregate class."""

    @staticmethod
    async def add_ticker_to_stock(ticker: str) -> UUID:
        """Create a new instance."""

        stocks = await Stocks.create(ticker=ticker, updated="Never", quotes=EntitySet())
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

    @staticmethod
    async def add_quotes(stock_uuid: str, quote: dict):
        stock = await Stocks.get(stock_uuid)
        quote = Quotes(close_value=quote["close"], volume=quote["volume"], when=quote["when"], ticker_name=stock.ticker)
        stock.quotes.add(quote)
        await stock.save()
