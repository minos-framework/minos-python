from dependency_injector.wiring import (
    Provide,
)

from src.queries.repository import (
    PaymentQueryServiceRepository,
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


class PaymentQueryService(QueryService):
    """PaymentQueryService class."""

    repository: PaymentQueryServiceRepository = Provide["payment_repository"]

    @enroute.rest.query("/payments", "GET")
    async def get_payment(self, request: Request) -> Response:
        """Get a Payment instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("PaymentCreated")
    async def payment_created(self, request: Request) -> None:
        """Handle the Payment creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("PaymentUpdated")
    async def payment_updated(self, request: Request) -> None:
        """Handle the Payment update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("PaymentDeleted")
    async def payment_deleted(self, request: Request) -> None:
        """Handle the Payment deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)
