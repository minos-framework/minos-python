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

    @enroute.rest.command("/wallet", "POST")
    async def create_wallet(self, request: Request) -> Response:
        """Create a new ``Wallet`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()
            uuid = await WalletAggregate.create_wallet(content["wallet_name"])
            return Response({"uuid": uuid, "wallet_name": content["wallet_name"]})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Wallet creation: {exc}")

    @enroute.rest.command("/wallet/ticker", "POST")
    async def add_ticker(self, request: Request) -> Response:
        """Create a new ``Wallet`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()
            wallet_uuid = content["wallet"]
            ticker = content["ticker"]
            flag = content["flag"]
            uuid = await WalletAggregate.add_ticker(wallet_uuid, ticker, flag)
            return Response({"uuid": uuid, "ticker": ticker, "flag": flag})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Wallet creation: {exc}")
