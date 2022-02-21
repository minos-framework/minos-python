from minos.aggregate import (
    Event,
)

from dependency_injector.wiring import (
    Provide,
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

from src import CoinbaseQueryServiceRepository


class CoinbaseQueryService(QueryService):
    """CoinbaseQueryService class."""

    repository: CoinbaseQueryServiceRepository = Provide["coinbase_repository"]

    @enroute.rest.query("/coinbases", "GET")
    async def get_coinbase(self, request: Request) -> Response:
        """Get a Coinbase instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("CreateCoinbaseWallet")
    async def coinbase_created(self, request: Request) -> None:
        """Handle the Coinbase creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()

        print(event)

    @enroute.broker.event("CoinbaseUpdated")
    async def coinbase_updated(self, request: Request) -> None:
        """Handle the Coinbase update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("CoinbaseDeleted")
    async def coinbase_deleted(self, request: Request) -> None:
        """Handle the Coinbase deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)
