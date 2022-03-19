import logging

from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)
from minos.aggregate import (
    Event,
)

from ..aggregates import (
    StocksAggregate,
)
import arrow
from alpha_vantage.timeseries import TimeSeries

logger = logging.getLogger(__name__)

class StocksCommandService(CommandService):
    """StocksCommandService class."""

    @enroute.rest.command("/stockss", "POST")
    @enroute.broker.command("CreateStocks")
    async def create_stocks(self, request: Request) -> Response:
        """Create a new ``Stocks`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            stocks = await StocksAggregate.create()
            return Response(stocks)
        except Exception as exc:
            raise ResponseException(f"An error occurred during Stocks creation: {exc}")

    @enroute.broker.event("WalletUpdated.tickers.create")
    async def set_stock_ticker(self, request: Request):
        event: Event = await request.content()
        for ticker in event['tickers']:
            await StocksAggregate.add_ticker_to_stock(ticker['ticker'])

    @enroute.periodic.event("* * * * *")
    async def get_stock_values(self, request: Request):
        tickers = StocksAggregate.get_all_tickers()
        if len(tickers) > 0:
            timeserie = TimeSeries(key="LPY6CZEYR6OIMRYA")
            for ticker in tickers:
                updated = ticker['updated']
                if updated == "Never":
                    time_now = arrow.utcnow().format('YYYY-MM-DD HH:mm:ss')
                    StocksAggregate.update_time_ticker(ticker['uuid'], time_now)
                data, metadata = await timeserie.get_intraday(ticker)
                logger.warning(metadata)
                logger.warning(data)
