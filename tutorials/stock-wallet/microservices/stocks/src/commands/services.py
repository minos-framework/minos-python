from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)

from ..aggregates import (
    StocksAggregate,
)


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