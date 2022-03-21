from dependency_injector.wiring import (
    Provide,
)
from src.queries.repository import (
    StocksQueryServiceRepository,
)

from minos.aggregate import (
    Event,
)
from minos.cqrs import (
    QueryService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)


class StocksQueryService(QueryService):
    """StocksQueryService class."""

    repository: StocksQueryServiceRepository = Provide["stocks_repository"]

    @enroute.rest.query("/stockss", "GET")
    async def get_stocks(self, request: Request) -> Response:
        """Get a Stocks instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("StocksCreated")
    async def stocks_created(self, request: Request) -> None:
        """Handle the Stocks creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("StocksUpdated")
    async def stocks_updated(self, request: Request) -> None:
        """Handle the Stocks update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("StocksDeleted")
    async def stocks_deleted(self, request: Request) -> None:
        """Handle the Stocks deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)
