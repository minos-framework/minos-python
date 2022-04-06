from abc import (
    abstractmethod,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    datetime,
)

from .....actions import (
    Action,
)


class EventRepositoryOperationFactory:
    """TODO"""

    @abstractmethod
    def build_create_table(self) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_submit_row(
        self,
        transaction_uuids: tuple[UUID],
        uuid: UUID,
        action: Action,
        name: str,
        version: int,
        data: bytes,
        created_at: datetime,
        transaction_uuid: UUID,
        lock: Optional[str],
        **kwargs,
    ) -> DatabaseOperation:
        """TODO"""

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def build_select_rows(
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
        """TODO"""

    @abstractmethod
    def build_select_max_id(self) -> DatabaseOperation:
        """TODO"""
