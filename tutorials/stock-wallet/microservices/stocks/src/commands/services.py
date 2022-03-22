import logging

import arrow
from alpha_vantage.async_support.timeseries import (
    TimeSeries,
)

from minos.aggregate import (
    Event,
)
from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    enroute,
)

from ..aggregates import (
    StocksAggregate,
)

logger = logging.getLogger(__name__)


class StocksCommandService(CommandService):
    """StocksCommandService class."""

    @enroute.broker.event("WalletUpdated.tickers.create")
    async def set_stock_ticker(self, request: Request):
        event: Event = await request.content()
        for ticker in event["tickers"]:
            await StocksAggregate.add_ticker_to_stock(ticker["ticker"])

    async def call_remote(self, ticker):
        timeserie = TimeSeries(key="LPY6CZEYR6OIMRYA")
        data, metadata = await timeserie.get_intraday(ticker)
        return data, metadata

    async def add_quote(self, ticker: str, when: str, quote: dict):
        await StocksAggregate.add_quotes(
            ticker,
            {"close": quote["4. close"], "volume": quote["5. volume"], "when": when},
        )

    @enroute.periodic.event("* * * * *")
    async def get_stock_values(self, request: Request):
        tickers = await StocksAggregate.get_all_tickers()
        if len(tickers) > 0:
            for ticker in tickers:
                updated = ticker["updated"]
                data, metadata = await self.call_remote(ticker["ticker"])
                last_data_refresh = metadata["3. Last Refreshed"]
                if updated == "Never":
                    data, metadata = await self.call_remote(ticker["ticker"])
                    await StocksAggregate.update_time_ticker(ticker["uuid"], last_data_refresh)
                    for when, stock_quote in data.items():
                        await self.add_quote(ticker["uuid"], when, stock_quote)
                else:
                    time_now_object = arrow.get(updated, "YYYY-MM-DD HH:mm:ss")
                    time_last_update = arrow.get(last_data_refresh, "YYYY-MM-DD HH:mm:ss")
                    diff = time_last_update - time_now_object
                    diff_in_seconds = diff.total_seconds()
                    if diff_in_seconds > 60:
                        await StocksAggregate.update_time_ticker(ticker["uuid"], last_data_refresh)
                        for when, stock_quote in data.items():
                            time_stock_loop = arrow.get(when, "YYYY-MM-DD HH:mm:ss")
                            diff_loop = last_data_refresh - time_stock_loop
                            diff_loop_seconds = diff_loop.total_seconds()
                            if diff_loop_seconds > 60:
                                await self.add_quote(ticker["uuid"], when, stock_quote)
