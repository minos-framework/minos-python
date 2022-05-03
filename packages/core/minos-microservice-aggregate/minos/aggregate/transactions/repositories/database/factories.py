from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from datetime import (
    datetime,
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
    DatabaseOperationFactory,
)

if TYPE_CHECKING:
    from ...entries import (
        TransactionStatus,
    )


class TransactionDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Transaction Database Operation Factory base class."""

    @abstractmethod
    def build_create(self) -> DatabaseOperation:
        """Build the database operation to create the snapshot table.

        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_submit(
        self, uuid: UUID, destination_uuid: UUID, status: TransactionStatus, event_offset: int, **kwargs
    ) -> DatabaseOperation:
        """Build the database operation to submit a row.

        :param uuid: The identifier of the transaction.
        :param destination_uuid: The identifier of the destination transaction.
        :param status: The status of the transaction.
        :param event_offset: The event offset of the transaction.
        :param kwargs: Additional named arguments.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_query(
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
        """Build the database operation to select rows.

        :param uuid: Transaction identifier equal to the given value.
        :param uuid_ne: Transaction identifier not equal to the given value
        :param uuid_in: Transaction identifier within the given values.
        :param destination_uuid: Destination Transaction identifier equal to the given value.
        :param status: Transaction status equal to the given value.
        :param status_in: Transaction status within the given values
        :param event_offset: Event offset equal to the given value.
        :param event_offset_lt: Event Offset lower than the given value
        :param event_offset_gt: Event Offset greater than the given value
        :param event_offset_le: Event Offset lower or equal to the given value
        :param event_offset_ge: Event Offset greater or equal to the given value
        :param updated_at: Updated at equal to the given value.
        :param updated_at_lt: Updated at lower than the given value.
        :param updated_at_gt: Updated at greater than the given value.
        :param updated_at_le: Updated at lower or equal to the given value.
        :param updated_at_ge: Updated at greater or equal to the given value.
        :param kwargs: Additional named arguments.
        :return: A ``DatabaseOperation`` instance.
        """
