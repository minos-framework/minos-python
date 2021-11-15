from collections import (
    namedtuple,
)
from pathlib import (
    Path,
)
from typing import (
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
    MinosBroker,
    MinosSagaManager,
    Model,
    current_datetime,
)
from minos.networks import (
    CommandStatus,
    EnrouteDecorator,
    Request,
    Response,
    WrappedRequest,
    enroute,
)

BASE_PATH = Path(__file__).parent


class FakeModel(DeclarativeModel):
    """For testing purposes"""

    text: str


FAKE_AGGREGATE_DIFF = FakeModel("Foo")

Message = namedtuple("Message", ["topic", "partition", "value"])


class FakeConsumer:
    """For testing purposes."""

    def __init__(self, messages=None):
        if messages is None:
            messages = [Message(topic="TicketAdded", partition=0, value=bytes())]
        self.messages = messages

    async def start(self):
        """For testing purposes."""

    async def stop(self):
        """For testing purposes."""

    def subscribe(self, *args, **kwargs):
        """For testing purposes."""

    def unsubscribe(self):
        """For testing purposes."""

    async def getmany(self, *args, **kwargs):
        return dict(enumerate(self.messages))

    async def __aiter__(self):
        for message in self.messages:
            yield message


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


class FakeSagaManager(MinosSagaManager):
    """For testing purposes."""

    async def _run_new(self, name: str, **kwargs) -> None:
        """For testing purposes."""

    async def _load_and_run(self, reply, **kwargs) -> None:
        """For testing purposes."""


class FakeBroker(MinosBroker):
    """For testing purposes."""

    def __init__(self):
        super().__init__()
        self.call_count = 0
        self.items = None
        self.topic = None
        self.saga = None
        self.reply_topic = None
        self.status = None

    async def send(
        self,
        items: list[Model],
        topic: str = None,
        saga: str = None,
        reply_topic: str = None,
        status: CommandStatus = None,
        **kwargs,
    ) -> None:
        """For testing purposes."""
        self.call_count += 1
        self.items = items
        self.topic = topic
        self.saga = saga
        self.reply_topic = reply_topic
        self.status = status


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
    def __get_enroute__(*args, **kwargs) -> dict[str, set[EnrouteDecorator]]:
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

    def __eq__(self, other) -> bool:
        return self._content == other._content

    def __repr__(self) -> str:
        return f"FakeRequest({self._content!r})"
