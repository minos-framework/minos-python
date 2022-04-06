from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Any,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    datetime,
)


class SnapshotRepositoryOperationFactory(ABC):
    """TODO"""

    @abstractmethod
    def build_create_table(self) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_delete_by_transactions(self, transaction_uuids: Iterable[UUID]) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_store_offset(self, value: int) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_get_offset(self) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_insert(
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
        """TODO"""
