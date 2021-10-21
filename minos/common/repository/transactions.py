from __future__ import (
    annotations,
)

from enum import (
    Enum,
)
from uuid import (
    UUID,
)

from .entries import (
    RepositoryEntry,
)


class RepositoryTransaction:
    """TODO"""

    def __init__(
        self,
        uuid: UUID,
        autocommit: bool = True,
        status: RepositoryTransactionStatus = None,
        entries: list[RepositoryEntry] = None,
    ):
        if status is None:
            status = RepositoryTransactionStatus.CREATED
        self.uuid = uuid
        self.autocommit = autocommit
        self.status = status
        self.entries = entries

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.autocommit:
            await self.commit()

    async def commit(self) -> None:
        """TODO"""


class RepositoryTransactionStatus(str, Enum):
    """TODO"""

    CREATED = "created"
    PENDING = "pending"
    COMMITTED = "committed"
