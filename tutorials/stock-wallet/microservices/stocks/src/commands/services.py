import logging

import pendulum
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
from polygon import RESTClient

logger = logging.getLogger(__name__)


class StocksCommandService(CommandService):
    """StocksCommandService class."""

    @enroute.broker.event("WalletUpdated.tickers.create")
    async def set_stock_ticker(self, request: Request):
        event: Event = await request.content()
        for ticker in event["tickers"]:
            if ticker['flag'] == "ticker":
                now = pendulum.now()
                await StocksAggregate.add_ticker_to_stock(ticker["ticker"], now.to_datetime_string())

    def call_remote(self, ticker, from_: str, to_: str):
        with RESTClient("") as client:
            resp = client.stocks_equities_aggregates(ticker, 1, "hour", from_, to_, adjusted=True, sort="asc",
                                                     limit=50000)
        return resp.results

    @enroute.periodic.event("* * * * *")
    async def get_stock_values(self, request: Request):
        tickers = await StocksAggregate.get_all_tickers()
        now = pendulum.now()
        now_minus_one_month = now.subtract(months=1)
        if len(tickers) > 0:
            for ticker in tickers:
                ticker_updated = pendulum.parse(ticker["updated"])
                results = self.call_remote(ticker["ticker"],
                                                 now_minus_one_month.to_date_string(),
                                                 now.to_date_string())
                for result in results:
                    result_date = pendulum.from_timestamp(result["t"]/1000)
                    # difference_ticker_result = ticker_updated.diff(result_date).in_hours()
                    if ticker_updated < result_date:
                        await StocksAggregate.update_time_ticker(ticker["uuid"], result_date.to_datetime_string())
                        logger.warning("Date time ticker updated")
                        when = result_date.to_datetime_string()
                        await StocksAggregate.add_quotes(ticker["uuid"], {"close": result['c'],
                                                                          "volume": result['v'],
                                                                          "when": when})
                        logger.warning("Added new quote")

