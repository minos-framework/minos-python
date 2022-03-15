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


class WalletQueryService(QueryService):
    """WalletQueryService class."""

    @enroute.rest.query("/wallets", "GET")
    async def get_wallet(self, request: Request) -> Response:
        """Get a Wallet instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("WalletCreated")
    async def wallet_created(self, request: Request) -> None:
        """Handle the Wallet creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("WalletUpdated")
    async def wallet_updated(self, request: Request) -> None:
        """Handle the Wallet update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("WalletDeleted")
    async def wallet_deleted(self, request: Request) -> None:
        """Handle the Wallet deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)
