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


class NotifierQueryService(QueryService):
    """NotifierQueryService class."""

    @enroute.rest.query("/notifiers", "GET")
    async def get_notifier(self, request: Request) -> Response:
        """Get a Notifier instance.

        :param request: A request instance..
        :return: A response exception.
        """
        raise ResponseException("Not implemented yet!")

    @enroute.broker.event("NotifierCreated")
    async def notifier_created(self, request: Request) -> None:
        """Handle the Notifier creation events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("NotifierUpdated")
    async def notifier_updated(self, request: Request) -> None:
        """Handle the Notifier update events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)

    @enroute.broker.event("NotifierDeleted")
    async def notifier_deleted(self, request: Request) -> None:
        """Handle the Notifier deletion events.

        :param request: A request instance containing the aggregate difference.
        :return: This method does not return anything.
        """
        event: Event = await request.content()
        print(event)