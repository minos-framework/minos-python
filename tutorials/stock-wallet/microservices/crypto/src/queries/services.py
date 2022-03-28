from dependency_injector.wiring import (
    Provide,
)

from src.queries.repository import (
    CryptoQueryServiceRepository,
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


class CryptoQueryService(QueryService):
    """CryptoQueryService class."""

    repository: CryptoQueryServiceRepository = Provide["crypto_repository"]

    @enroute.rest.query("/cryptos", "GET")
    async def get_crypto(self, request: Request) -> Response:
        """Get a Crypto instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    # @enroute.broker.event("CryptoCreated")
    # async def crypto_created(self, request: Request) -> None:
    #     """Handle the Crypto creation events.
    #
    #     :param request: A request instance containing the aggregate difference.
    #     :return: This method does not return anything.
    #     """
    #     event: Event = await request.content()
    #     print(event)

    # @enroute.broker.event("CryptoUpdated")
    # async def crypto_updated(self, request: Request) -> None:
    #     """Handle the Crypto update events.
    #
    #     :param request: A request instance containing the aggregate difference.
    #     :return: This method does not return anything.
    #     """
    #     event: Event = await request.content()
    #     print(event)

    # @enroute.broker.event("CryptoDeleted")
    # async def crypto_deleted(self, request: Request) -> None:
    #     """Handle the Crypto deletion events.
    #
    #     :param request: A request instance containing the aggregate difference.
    #     :return: This method does not return anything.
    #     """
    #     event: Event = await request.content()
    #     print(event)
