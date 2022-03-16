from dependency_injector.wiring import Provide
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

from src import WalletQueryServiceRepository


class WalletQueryService(QueryService):
    """WalletQueryService class."""
    repository: WalletQueryServiceRepository = Provide["wallet_repository"]

    @enroute.rest.query("/wallets", "GET")
    async def get_wallets(self, request: Request) -> Response:
        """Get a Wallet instance.

        :param request: A request instance..
        :return: A response exception.
        """
        wallets = self.repository.get_wallets()
        raise Response(wallets)

    @enroute.rest.query("/wallets/{uuid}/tickers", "GET")
    async def get_wallets_tickers(self, request: Request) -> Response:
        """Get a Wallet instance.

        :param request: A request instance..
        :return: A response exception.
        """
        params = await request.params()
        tickers = self.repository.get_tickers(params['uuid'])
        raise Response(tickers)

    @enroute.broker.event("WalletCreated")
    async def wallet_created(self, request: Request) -> None:
        """Handle the Wallet creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        self.repository.create_wallet(event.get_field('name'), event['uuid'])

    @enroute.broker.event("WalletUpdated.tickers.create")
    async def wallet_add_tickers(self, request: Request) -> None:
        """Handle the Wallet update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        for ticker in event['tickers']:
            self.repository.add_tickers(event['uuid'], ticker)
