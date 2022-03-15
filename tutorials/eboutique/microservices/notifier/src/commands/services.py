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
    NotifierAggregate,
)


class NotifierCommandService(CommandService):
    """NotifierCommandService class."""

    @enroute.rest.command("/notifiers", "POST")
    @enroute.broker.command("CreateNotifier")
    async def create_notifier(self, request: Request) -> Response:
        """Create a new ``Notifier`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            uuid = await NotifierAggregate.create()
            return Response({"uuid": uuid})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Notifier creation: {exc}")