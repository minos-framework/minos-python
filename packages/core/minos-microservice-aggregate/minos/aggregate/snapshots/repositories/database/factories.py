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
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    DatabaseOperationFactory,
)

from ....queries import (
    _Condition,
    _Ordering,
)


class SnapshotDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Snapshot Database Operation Factory class."""

    @abstractmethod
    def build_create(self) -> DatabaseOperation:
        """Build the database operation to create the snapshot table.

        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_delete(self, transaction_uuids: Iterable[UUID]) -> DatabaseOperation:
        """Build the database operation to delete rows by transaction identifiers.

        :param transaction_uuids: The transaction identifiers.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_submit(
        self,
        uuid: UUID,
        name: str,
        version: int,
        schema: bytes,
        data: dict[str, Any],
        created_at: datetime,
        updated_at: datetime,
        transaction_uuid: UUID,
    ) -> DatabaseOperation:
        """Build the insert database operation.

        :param uuid: The identifier of the entity.
        :param name: The name of the entity.
        :param version: The version of the entity.
        :param schema: The schema of the entity.
        :param data: The data of the entity.
        :param created_at: The creation datetime.
        :param updated_at: The last update datetime.
        :param transaction_uuid: The transaction identifier.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_query(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        transaction_uuids: tuple[UUID, ...],
        exclude_deleted: bool,
    ) -> DatabaseOperation:
        """Build the query database operation.

        :param name: Class name of the ``RootEntity``.
        :param condition: The condition that must be satisfied by the ``RootEntity`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param transaction_uuids: The transaction within the operation is performed. If not any value is provided, then
            the transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param exclude_deleted: If ``True``, deleted ``RootEntity`` entries are included, otherwise deleted
            ``RootEntity`` entries are filtered.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_submit_offset(self, value: int) -> DatabaseOperation:
        """Build the database operation to store the offset.

        :param value: The value to be stored as the new offset.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_query_offset(self) -> DatabaseOperation:
        """Build the database operation to get the current offset.

        :return: A ``DatabaseOperation`` instance.
        """
