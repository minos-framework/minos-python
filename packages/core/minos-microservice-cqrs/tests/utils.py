from __future__ import (
    annotations,
)

from collections.abc import (
    AsyncIterator,
    Iterable,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
    Optional,
)

from minos.aggregate import (
    ExternalEntity,
    Ref,
    RootEntity,
)
from minos.common import (
    DatabaseClient,
    DatabaseOperation,
    LockDatabaseOperationFactory,
    ManagementDatabaseOperationFactory,
)
from minos.cqrs import (
    CommandService,
    QueryService,
    Service,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)

BASE_PATH = Path(__file__).parent


class FakeService(Service):
    """For testing purposes."""


class FakeQueryService(QueryService):
    """For testing purposes."""

    @enroute.broker.query("FindFoo")
    async def find_foo(self, request: Request) -> Response:
        """For testing purpose"""


class FakeCommandService(CommandService):
    """For testing purposes."""

    @enroute.broker.command("CreateFoo")
    async def create_foo(self, request: Request) -> Response:
        """For testing purpose"""


class Foo(RootEntity):
    """For testing purposes"""

    bar: Ref[Bar]


class Bar(ExternalEntity):
    """For testing purposes"""

    name: str


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


FakeDatabaseClient.register_factory(LockDatabaseOperationFactory, FakeLockDatabaseOperationFactory)


class FakeManagementDatabaseOperationFactory(ManagementDatabaseOperationFactory):
    """For testing purposes"""

    def build_create(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("create")

    def build_delete(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return FakeDatabaseOperation("delete")


FakeDatabaseClient.register_factory(ManagementDatabaseOperationFactory, FakeManagementDatabaseOperationFactory)
