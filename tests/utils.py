from collections import (
    namedtuple,
)
from datetime import (
    timedelta,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
    Callable,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    DeclarativeModel,
)
from minos.networks import (
    EnrouteHandleDecorator,
    Request,
    Response,
    WrappedRequest,
    enroute,
)

BASE_PATH = Path(__file__).parent


class FakeModel(DeclarativeModel):
    """For testing purposes"""

    text: str


Message = namedtuple("Message", ["topic", "partition", "value"])


class FakeDispatcher:
    """For testing purposes"""

    def __init__(self):
        self.setup_count = 0
        self.setup_dispatch = 0
        self.setup_destroy = 0

    async def setup(self):
        """For testing purposes."""
        self.setup_count += 1

    async def dispatch(self):
        """For testing purposes."""
        self.setup_dispatch += 1

    async def destroy(self):
        """For testing purposes."""
        self.setup_destroy += 1


async def fake_middleware(request: Request, inner: Callable) -> Optional[Response]:
    """For testing purposes."""
    response = await inner(request)
    if response is not None:
        return Response(f"_{await response.content()}_")
    return response


class FakeService:
    """For testing purposes."""

    @staticmethod
    def _pre_query_handle(request: Request) -> Request:
        return request

    @staticmethod
    async def _pre_event_handle(request: Request) -> Request:
        return WrappedRequest(request, lambda content: f"[{content}]")

    @staticmethod
    def _post_command_handle(response: Response) -> Response:
        return response

    @staticmethod
    async def _post_query_handle(response: Response) -> Response:
        return Response(f"({await response.content()})")

    # noinspection PyUnusedLocal
    @enroute.rest.command(url="orders/", method="GET")
    @enroute.broker.command(topic="CreateTicket")
    @enroute.broker.command(topic="AddTicket")
    def create_ticket(self, request: Request) -> Response:
        """For testing purposes."""
        return Response("Create Ticket")

    @create_ticket.check()
    def check_create_ticket_1(self, request: Request) -> bool:
        return True

    @create_ticket.check(max_attempts=3, delay=timedelta(seconds=1))
    def check_create_ticket_2(self, request: Request) -> bool:
        return True

    # noinspection PyUnusedLocal
    @classmethod
    @enroute.rest.command(url="orders/", method="DELETE")
    @enroute.broker.command(topic="DeleteTicket")
    def delete_ticket(cls, request: Request) -> None:
        """For testing purposes."""
        return

    @enroute.rest.query(url="tickets/", method="GET")
    @enroute.broker.query(topic="GetTickets")
    async def get_tickets(self, request: Request) -> Response:
        """For testing purposes."""
        return Response(": ".join(("Get Tickets", await request.content(),)))

    @staticmethod
    @enroute.broker.event(topic="TicketAdded")
    async def ticket_added(request: Request) -> Response:
        """For testing purposes."""
        return Response(": ".join(("Ticket Added", await request.content(),)))

    @enroute.periodic.event("@daily")
    async def send_newsletter(self, request: Request):
        """For testing purposes."""
        return Response("newsletter sent!")

    @enroute.periodic.event("@daily")
    async def check_inactive_users(self, request: Request):
        """For testing purposes."""
        return Response("checked inactive users!")

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def bar(self, request: Request):
        """For testing purposes."""
        return Response("bar")


class FakeServiceWithGetEnroute:
    @staticmethod
    def __get_enroute__(*args, **kwargs) -> dict[str, set[EnrouteHandleDecorator]]:
        return {"create_foo": {enroute.broker.command(topic="CreateFoo")}}

    def create_foo(self, request: Request) -> Response:
        """For testing purposes."""


class FakeRequest(Request):
    """For testing purposes"""

    def __init__(self, content):
        super().__init__()
        self._content = content

    @cached_property
    def user(self) -> Optional[UUID]:
        return uuid4()

    async def content(self, **kwargs):
        """For testing purposes"""
        return self._content

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self._content == other._content

    def __repr__(self) -> str:
        return f"FakeRequest({self._content!r})"
