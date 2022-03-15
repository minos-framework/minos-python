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
    CheckoutAggregate,
)


class CheckoutCommandService(CommandService):
    """CheckoutCommandService class."""

    @enroute.rest.command("/checkouts", "POST")
    @enroute.broker.command("CreateCheckout")
    async def create_checkout(self, request: Request) -> Response:
        """Create a new ``Checkout`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            uuid = await CheckoutAggregate.create()
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Checkout creation: {exc}")
