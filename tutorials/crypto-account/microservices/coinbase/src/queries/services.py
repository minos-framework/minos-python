from dependency_injector.wiring import (
    Provide,
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

from src import (
    CoinbaseQueryServiceRepository,
)


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

    @enroute.broker.event("CoinbaseCreated")
    async def coinbase_created(self, request: Request) -> None:
        """Handle the Coinbase creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        self.repository.add_wallet(
            uuid=event["uuid"],
            user=event.get_one("user"),
            api_key=event.get_one("api_key"),
            api_secret=event.get_one("api_secret"),
        )

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
