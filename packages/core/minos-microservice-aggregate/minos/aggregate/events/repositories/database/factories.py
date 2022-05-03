from abc import (
    ABC,
    abstractmethod,
)
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
    DatabaseOperationFactory,
)

from ....actions import (
    Action,
)


class EventDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Event Database Operation Factory base class."""

    @abstractmethod
    def build_create(self) -> DatabaseOperation:
        """Build the database operation to create the event table.

        :return: A ``DatabaseOperation`` instance.s
        """

    @abstractmethod
    def build_submit(
        self,
        transaction_uuids: Iterable[UUID],
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
        """Build the database operation to submit a row into the event table.

        :param transaction_uuids: The sequence of nested transaction in on top of the current event's transaction.
        :param uuid: The identifier of the entity.
        :param action: The action of the event.
        :param name: The name of the entity.
        :param version: The version of the entity
        :param data: The data of the event.
        :param created_at: The creation datetime.
        :param transaction_uuid: The identifier of the transaction.
        :param lock: The lock identifier.
        :param kwargs: Additional named arguments.
        :return: A ``DatabaseOperation`` instance.
        """

    # noinspection PyShadowingBuiltins
    @abstractmethod
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
        """Build the database operation to select rows.

        :param uuid: The identifier must be equal to the given value.
        :param name: The classname must be equal to the given value.
        :param version: The version must be equal to the given value.
        :param version_lt: The version must be lower than the given value.
        :param version_gt: The version must be greater than the given value.
        :param version_le: The version must be lower or equal to the given value.
        :param version_ge: The version must be greater or equal to the given value.
        :param id: The entry identifier must be equal to the given value.
        :param id_lt: The entry identifier must be lower than the given value.
        :param id_gt: The entry identifier must be greater than the given value.
        :param id_le: The entry identifier must be lower or equal to the given value.
        :param id_ge: The entry identifier must be greater or equal to the given value.
        :param transaction_uuid: The transaction identifier must be equal to the given value.
        :param transaction_uuid_ne: The transaction identifier must be distinct of the given value.
        :param transaction_uuid_in: The destination transaction identifier must be equal to one of the given values.

        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_query_offset(self) -> DatabaseOperation:
        """Build the database operation to get the maximum identifier.

        :return: A ``DatabaseOperation`` instance.
        """
