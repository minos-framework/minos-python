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
    WalletAggregate,
)


class WalletCommandService(CommandService):
    """WalletCommandService class."""

    @enroute.rest.command("/wallets", "POST")
    @enroute.broker.command("CreateWallet")
    async def create_wallet(self, request: Request) -> Response:
        """Create a new ``Wallet`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            uuid = await WalletAggregate.create()
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Wallet creation: {exc}")