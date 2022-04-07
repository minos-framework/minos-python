from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    datetime, DatabaseOperationFactory,
)

if TYPE_CHECKING:
    from ....entries import (
        TransactionStatus,
    )


class TransactionDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """TODO"""

    @abstractmethod
    def build_create_table(self) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_submit_row(
        self,
        uuid: UUID,
        destination_uuid: UUID,
        status: TransactionStatus,
        event_offset: int,
    ) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_select_rows(
        self,
        uuid: Optional[UUID] = None,
        uuid_ne: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID]] = None,
        destination_uuid: Optional[UUID] = None,
        status: Optional[str] = None,
        status_in: Optional[tuple[str]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        updated_at: Optional[datetime] = None,
        updated_at_lt: Optional[datetime] = None,
        updated_at_gt: Optional[datetime] = None,
        updated_at_le: Optional[datetime] = None,
        updated_at_ge: Optional[datetime] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """TODO"""
