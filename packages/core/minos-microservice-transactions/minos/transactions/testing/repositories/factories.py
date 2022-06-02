from collections.abc import (
    Iterable,
)
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

from ...entries import (
    TransactionStatus,
)
from ...repositories import (
    TransactionDatabaseOperationFactory,
)


class MockedTransactionDatabaseOperationFactory(TransactionDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("create")

    def build_submit(
        self, uuid: UUID, destination_uuid: UUID, status: TransactionStatus, **kwargs
    ) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("submit")

    def build_query(
        self,
        uuid: Optional[UUID] = None,
        uuid_ne: Optional[UUID] = None,
        uuid_in: Optional[Iterable[UUID]] = None,
        destination_uuid: Optional[UUID] = None,
        status: Optional[str] = None,
        status_in: Optional[Iterable[str]] = None,
        updated_at: Optional[datetime] = None,
        updated_at_lt: Optional[datetime] = None,
        updated_at_gt: Optional[datetime] = None,
        updated_at_le: Optional[datetime] = None,
        updated_at_ge: Optional[datetime] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """For testing purposes."""
        return MockedDatabaseOperation("select")


MockedDatabaseClient.set_factory(TransactionDatabaseOperationFactory, MockedTransactionDatabaseOperationFactory)
