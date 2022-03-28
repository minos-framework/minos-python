import logging
import time

import pendulum
from minos.cqrs import (
    CommandService,
)
from minos.common import ModelType

from minos.networks import (
    Request,
    enroute,
    BrokerMessageV1,
    BrokerMessageV1Payload
)
from minos.aggregate import (
    Event
)
from ..aggregates import (
    CryptoAggregate,
)
import ccxt
logger = logging.getLogger(__name__)
QuoteContent = ModelType.build("QuoteContent", {"ticker": str, "close": float, "volume": float, "when": str})


class CryptoCommandService(CommandService):
    """CryptoCommandService class."""

    @enroute.broker.event("WalletUpdated.tickers.create")
    async def set_crypto_coin(self, request: Request):
        event: Event = await request.content()
        for ticker in event["tickers"]:
            logger.warning(ticker)
            if ticker['flag'] == "crypto":
                now = pendulum.parse('1975-08-27T05:00:00')
                logger.warning("Added crypto to stock")
                await CryptoAggregate.add_crypto_to_stock(ticker["ticker"], now.to_datetime_string())

    def call_remote(self, ticker, _from: float):
        kraken = ccxt.kraken({
            'enableRateLimit': True,
            'apiKey': 'MilT+OBywOBlgMPZnsCNSSlwxOGEtGfhnNXJl0pi87MCltuKRA+IoiVc',
            'secret': 'sGQpkVLH6sIPy7sjuro1sFiMYHC+hhN7j/bWrNr+AHdTIZNR8jb+13+ZhVevo5hyCxRijUqhUWG47Ox0SokOig==',
        }
        )
        now = kraken.milliseconds()
        timeframe = "1h"
        limit = 700
        timeframe_duration_in_seconds = kraken.parse_timeframe(timeframe)
        timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
        timedelta = limit * timeframe_duration_in_ms
        since = kraken.parse8601(_from)
        logger.info("Since {}".format(since))
        kraken.load_markets()
        data = []
        fetch_since = since
        while fetch_since < now:
            logger.info("crawling kraken")
            try:
                values = kraken.fetch_ohlcv(ticker, timeframe, since, limit)
                fetch_since = (values[-1][0] + 3600000) if len(values) else (fetch_since + timedelta)
                data = data + values
                if len(values):
                    logger.info("{} candles in total from {} to {}".format(len(values), kraken.iso8601(values[0][0]),
                                                                           kraken.iso8601(values[-1][0])))
                else:
                    logger.info("{} candles in total from {}".format(len(values), kraken.iso8601(fetch_since)))
            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
                logger.error(error)
                time.sleep(30)
        return kraken.filter_by_since_limit(data, since, None, key=0)

    @enroute.periodic.event("* * * * *")
    async def get_crypto_values(self, request: Request):
        tickers = await CryptoAggregate.get_all_tickers()
        logger.warning("Periodic call Crypto----------")
        now = pendulum.now()
        now_minus_one_month = now.subtract(months=1)
        if len(tickers) > 0:
            for ticker in tickers:
                logger.warning("Num tickers {}".format(len(tickers)))
                ticker_updated = pendulum.parse(ticker["updated"])
                results = self.call_remote(ticker["ticker"], now_minus_one_month.to_datetime_string())
                for result in results:
                    result_date = pendulum.from_timestamp(result[0] / 1000)
                    if ticker_updated < result_date:
                        await CryptoAggregate.update_time_ticker(ticker["uuid"], result_date.to_datetime_string())
                        when = result_date.to_datetime_string()
                        message = BrokerMessageV1(
                            "QuotesChannel", BrokerMessageV1Payload(QuoteContent(ticker["ticker"], result[4],
                                                                                 result[5], when))
                        )
                        await self.broker_publisher.send(message)
