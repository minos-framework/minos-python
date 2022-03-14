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


class CurrencyQueryService(QueryService):
    """CurrencyQueryService class."""

    @enroute.rest.query("/currencys", "GET")
    async def get_currency(self, request: Request) -> Response:
        """Get a Currency instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("CurrencyCreated")
    async def currency_created(self, request: Request) -> None:
        """Handle the Currency creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("CurrencyUpdated")
    async def currency_updated(self, request: Request) -> None:
        """Handle the Currency update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("CurrencyDeleted")
    async def currency_deleted(self, request: Request) -> None:
        """Handle the Currency deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)