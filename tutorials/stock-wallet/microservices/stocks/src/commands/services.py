import logging

import pendulum
from polygon import (
    RESTClient,
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
    BrokerMessageV1,
    BrokerMessageV1Payload
)

from ..aggregates import (
    StocksAggregate,
)

from minos.common import ModelType

logger = logging.getLogger(__name__)

QuoteContent = ModelType.build("QuoteContent", {"ticker": str, "close": float, "volume": float, "when": str})


class StocksCommandService(CommandService):
    """StocksCommandService class."""

    @enroute.broker.event("WalletUpdated.tickers.create")
    async def set_stock_ticker(self, request: Request):
        event: Event = await request.content()
        for ticker in event["tickers"]:
            logger.warning(ticker)
            if ticker['flag'] == "ticker":
                now = pendulum.parse('1975-08-27T05:00:00')
                logger.warning("Added ticker to stock")
                await StocksAggregate.add_ticker_to_stock(ticker["ticker"], now.to_datetime_string())

    def call_remote(self, ticker, from_: str, to_: str):
        with RESTClient("X0Mb2GLCf84iE3intKsKV5f9EDWvenNR") as client:
            resp = client.stocks_equities_aggregates(
                ticker, 1, "hour", from_, to_, adjusted=True, sort="asc", limit=50000
            )
        return resp.results

    @enroute.periodic.event("* * * * *")
    async def get_stock_values(self, request: Request):
        tickers = await StocksAggregate.get_all_tickers()
        now = pendulum.now()
        logger.warning("Called Periodic")
        now_minus_one_month = now.subtract(months=1)
        if len(tickers) > 0:
            for ticker in tickers:
                logger.warning("Called Ticker Remote")
                ticker_updated = pendulum.parse(ticker["updated"])
                results = self.call_remote(ticker["ticker"],
                                           now_minus_one_month.to_date_string(),
                                           now.to_date_string())

                for result in results:
                    result_date = pendulum.from_timestamp(result["t"] / 1000)
                    if ticker_updated < result_date:
                        await StocksAggregate.update_time_ticker(ticker["uuid"], result_date.to_datetime_string())
                        logger.warning("Date time ticker updated")
                        when = result_date.to_datetime_string()
                        message = BrokerMessageV1(
                            "QuotesChannel", BrokerMessageV1Payload(QuoteContent(ticker["ticker"], result['c'],
                                                                                 result['v'], when))
                        )
                        await self.broker_publisher.send(message)
                        logger.warning("Added new quote")
