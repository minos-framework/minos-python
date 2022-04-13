from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from collections.abc import (
    AsyncIterator,
    Iterable,
)
from datetime import (
    timedelta,
)
from functools import (
    total_ordering,
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
)

from minos.common import (
    DatabaseClient,
    DatabaseOperation,
    DeclarativeModel,
    LockDatabaseOperationFactory,
    ManagementDatabaseOperationFactory,
)
from minos.common.testing import (
    MinosTestCase,
)
from minos.networks import (
    BrokerPublisherQueueDatabaseOperationFactory,
    BrokerQueueDatabaseOperationFactory,
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
    BrokerSubscriberQueueDatabaseOperationFactory,
    EnrouteDecorator,
    HttpConnector,
    Request,
    Response,
    WrappedRequest,
    enroute,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class NetworksTestCase(MinosTestCase, ABC):
    def get_config_file_path(self):
        return CONFIG_FILE_PATH


class FakeDatabaseClient(DatabaseClient):
    """For testing purposes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = kwargs
        self._response = tuple()

    async def _is_valid(self, **kwargs) -> bool:
        """For testing purposes"""
        return True

    async def _reset(self, **kwargs) -> None:
        """For testing purposes"""
        self._response = tuple()

    async def _execute(self, operation: FakeDatabaseOperation) -> None:
        """For testing purposes"""
        self._response = operation.response

    async def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        """For testing purposes"""
        for value in self._response:
            yield value


class FakeDatabaseOperation(DatabaseOperation):
    """For testing purposes"""

    def __init__(self, content: str, response: Optional[Iterable[Any]] = tuple(), *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content = content
        self.response = tuple(response)


class FakeLockDatabaseOperationFactory(LockDatabaseOperationFactory):
    """For testing purposes"""

    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("acquire")

    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("release")


FakeDatabaseClient.set_factory(LockDatabaseOperationFactory, FakeLockDatabaseOperationFactory)


class FakeManagementDatabaseOperationFactory(ManagementDatabaseOperationFactory):
    """For testing purposes"""

    def build_create(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("create")

    def build_delete(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("delete")


FakeDatabaseClient.set_factory(ManagementDatabaseOperationFactory, FakeManagementDatabaseOperationFactory)


class FakeBrokerQueueDatabaseOperationFactory(BrokerQueueDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("create_queue_table")

    def build_mark_processed(self, id_: int) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("update_not_processed")

    def build_delete(self, id_: int) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("delete_processed")

    def build_mark_processing(self, ids: Iterable[int]) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("mark_processing")

    def build_count(self, retry: int, *args, **kwargs) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("count_not_processed")

    def build_submit(self, topic: str, data: bytes) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("insert")

    def build_query(self, retry: int, records: int, *args, **kwargs) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("select_not_processed")


class FakeBrokerPublisherQueueDatabaseOperationFactory(
    BrokerPublisherQueueDatabaseOperationFactory, FakeBrokerQueueDatabaseOperationFactory
):
    """For testing purposes"""


FakeDatabaseClient.set_factory(
    BrokerPublisherQueueDatabaseOperationFactory, FakeBrokerPublisherQueueDatabaseOperationFactory
)


class FakeBrokerSubscriberQueueDatabaseOperationFactory(
    BrokerSubscriberQueueDatabaseOperationFactory, FakeBrokerQueueDatabaseOperationFactory
):
    """For testing purposes"""

    def build_count(self, retry: int, topics: Iterable[str] = tuple(), *args, **kwargs) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("count_not_processed")

    def build_query(
        self, retry: int, records: int, topics: Iterable[str] = tuple(), *args, **kwargs
    ) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("select_not_processed")


FakeDatabaseClient.set_factory(
    BrokerSubscriberQueueDatabaseOperationFactory, FakeBrokerSubscriberQueueDatabaseOperationFactory
)


class FakeBrokerSubscriberDuplicateValidatorDatabaseOperationFactory(
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory
):
    """For testing purposes"""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("create_table")

    def build_submit(self, topic: str, uuid: UUID) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("insert_row")


FakeDatabaseClient.set_factory(
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
    FakeBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)


@total_ordering
class FakeModel(DeclarativeModel):
    """For testing purposes"""

    data: Any

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        return isinstance(other, type(self)) and self.data < other.data


class FakeAsyncIterator:
    """For testing purposes."""

    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


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
    @enroute.rest.command(path="orders/", method="GET")
    @enroute.broker.command(topic="CreateTicket")
    @enroute.broker.command(topic="AddTicket")
    def create_ticket(self, request: Request) -> Response:
        """For testing purposes."""
        return Response("Create Ticket")

    @create_ticket.check(max_attempts=1, delay=0.5)
    def check_create_ticket_1(self, request: Request) -> bool:
        return True

    @create_ticket.check(delay=timedelta(seconds=1))
    def check_create_ticket_2(self, request: Request) -> bool:
        return True

    # noinspection PyUnusedLocal
    @classmethod
    @enroute.rest.command(path="orders/", method="DELETE")
    @enroute.broker.command(topic="DeleteTicket")
    def delete_ticket(cls, request: Request) -> None:
        """For testing purposes."""
        return

    @classmethod
    @delete_ticket.__func__.check()
    def check_classmethod(cls, request: Request) -> bool:
        return True

    @enroute.rest.query(path="tickets/", method="GET")
    @enroute.broker.query(topic="GetTickets")
    async def get_tickets(self, request: Request) -> Response:
        """For testing purposes."""
        return Response(": ".join(("Get Tickets", await request.content())))

    @create_ticket.check()
    @get_tickets.check()
    def check_multiple(self, request: Request) -> bool:
        return True

    @staticmethod
    @enroute.broker.event(topic="TicketAdded")
    async def ticket_added(request: Request) -> Response:
        """For testing purposes."""
        return Response(": ".join(("Ticket Added", await request.content())))

    @staticmethod
    @ticket_added.__func__.check()
    def check_static(request: Request) -> bool:
        return True

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


class FakeServiceWithKwargs:
    """For testing purposes."""

    @enroute.rest.query(
        path="tickets/",
        method="GET",
        authorized=True,
        allowed_groups=[
            "super_admin",
            "admin",
        ],
    )
    @enroute.broker.query(topic="GetTickets")
    async def get_tickets(self, request: Request) -> Response:
        """For testing purposes."""
        return Response(": ".join(("Get Tickets", await request.content())))


class FakeServiceWithGetEnroute:
    """For testing purposes."""

    @staticmethod
    def __get_enroute__(*args, **kwargs) -> dict[str, set[EnrouteDecorator]]:
        return {"create_foo": {enroute.broker.command(topic="CreateFoo")}}

    def create_foo(self, request: Request) -> Response:
        """For testing purposes."""


class FakeHttpConnector(HttpConnector[Any, Any]):
    """For testing purposes."""

    def _mount_route(self, path: str, method: str, adapted_callback: Callable):
        """For testing purposes."""

    async def _build_request(self, request: Any) -> Request:
        """For testing purposes."""

    async def _build_response(self, response: Optional[Response]) -> Any:
        """For testing purposes."""

    async def _build_error_response(self, message: str, status: int) -> Any:
        """For testing purposes."""

    async def _start(self) -> None:
        """For testing purposes."""

    async def _stop(self) -> None:
        """For testing purposes."""
