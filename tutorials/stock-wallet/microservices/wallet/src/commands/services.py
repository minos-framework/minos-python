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

    @enroute.rest.command("/wallet/ticker", "POST")
    @enroute.broker.command("AddTickerToWallet")
    async def create_wallet(self, request: Request) -> Response:
        """Create a new ``Wallet`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()
            uuid = await WalletAggregate.create(content['tinker'])
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Wallet creation: {exc}")
