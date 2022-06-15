from collections.abc import (
    Iterable,
)
from datetime import (
    datetime,
)
from typing import (
    Any,
    Optional,
    Union,
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

from ....queries import (
    _Condition,
    _Ordering,
)
from ....snapshots import (
    SnapshotDatabaseOperationFactory,
)


class MockedSnapshotDatabaseOperationFactory(SnapshotDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("create")

    def build_delete(self, transaction_uuids: Iterable[UUID]) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("delete")

    def build_submit(
        self,
        uuid: UUID,
        type_: str,
        version: int,
        schema: Optional[Union[list[dict[str, Any]], dict[str, Any]]],
        data: Optional[dict[str, Any]],
        created_at: datetime,
        updated_at: datetime,
        transaction_uuid: UUID,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("insert")

    def build_query(
        self,
        type_: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        transaction_uuids: tuple[UUID, ...],
        exclude_deleted: bool,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("query")

    def build_submit_offset(self, value: int) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("store_offset")

    def build_query_offset(self) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("get_offset")


MockedDatabaseClient.set_factory(SnapshotDatabaseOperationFactory, MockedSnapshotDatabaseOperationFactory)
