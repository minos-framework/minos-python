from __future__ import (
    annotations,
)

from contextvars import (
    ContextVar,
)
from enum import (
    Enum,
)
from typing import (
    TYPE_CHECKING,
    Final,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

if TYPE_CHECKING:
    from .abc import (
        MinosRepository,
    )

TRANSACTION_CONTEXT_VAR: Final[ContextVar[Optional[RepositoryTransaction]]] = ContextVar("transaction", default=None)


class RepositoryTransaction:
    """TODO"""

    def __init__(
        self,
        repository: MinosRepository,
        uuid: Optional[UUID] = None,
        autocommit: bool = True,
        status: RepositoryTransactionStatus = None,
    ):
        if uuid is None:
            uuid = uuid4()
        if status is None:
            status = RepositoryTransactionStatus.CREATED

        self.repository = repository
        self.uuid = uuid
        self.autocommit = autocommit
        self.status = status
        self._token = None

    async def __aenter__(self):
        if TRANSACTION_CONTEXT_VAR.get() is not None:
            raise ValueError()
        self._token = TRANSACTION_CONTEXT_VAR.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._token is not None:
            TRANSACTION_CONTEXT_VAR.reset(self._token)
        else:
            TRANSACTION_CONTEXT_VAR.set(None)

        if self.autocommit:
            await self.commit()

    async def commit(self) -> None:
        """TODO"""
        await self.repository.commit(self)

    def __repr__(self):
        return f"{type(self).__name__}(uuid={self.uuid!r}, entries={self.entries!r})"


class RepositoryTransactionStatus(str, Enum):
    """TODO"""

    CREATED = "created"
    PENDING = "pending"
    COMMITTED = "committed"
