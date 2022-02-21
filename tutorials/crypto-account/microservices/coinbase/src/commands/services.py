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
    CoinbaseAggregate,
)


class CoinbaseCommandService(CommandService):
    """CoinbaseCommandService class."""

    @enroute.rest.command("/coinbase", "POST")
    @enroute.broker.command("CreateCoinbaseWallet")
    async def create_coinbase(self, request: Request) -> Response:
        """Create a new ``Coinbase`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()
            uuid = await CoinbaseAggregate.create()
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Coinbase creation: {exc}")
