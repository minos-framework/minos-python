from datetime import (
    datetime,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
)
from minos.common.testing import (
    MockedDatabaseClient,
    MockedDatabaseOperation,
)

from ....actions import (
    Action,
)
from ....events import (
    EventDatabaseOperationFactory,
)


class MockedEventDatabaseOperationFactory(EventDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("create")

    def build_submit(
        self,
        transaction_uuids: tuple[UUID],
        uuid: UUID,
        action: Action,
        name: str,
        version: int,
        data: bytes,
        created_at: datetime,
        transaction_uuid: UUID,
        lock: Optional[int],
        **kwargs,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("submit")

    def build_query(
        self,
        uuid: Optional[UUID] = None,
        name: Optional[str] = None,
        version: Optional[int] = None,
        version_lt: Optional[int] = None,
        version_gt: Optional[int] = None,
        version_le: Optional[int] = None,
        version_ge: Optional[int] = None,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_le: Optional[int] = None,
        id_ge: Optional[int] = None,
        transaction_uuid: Optional[UUID] = None,
        transaction_uuid_ne: Optional[UUID] = None,
        transaction_uuid_in: Optional[tuple[UUID, ...]] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("select_rows")

    def build_query_offset(self) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("select_max_id")


MockedDatabaseClient.set_factory(EventDatabaseOperationFactory, MockedEventDatabaseOperationFactory)
