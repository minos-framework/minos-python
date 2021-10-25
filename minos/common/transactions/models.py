from __future__ import (
    annotations,
)

import logging
from enum import (
    Enum,
)
from typing import (
    TYPE_CHECKING,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from ..exceptions import (
    MinosRepositoryConflictException,
)
from .contextvars import (
    TRANSACTION_CONTEXT_VAR,
)

if TYPE_CHECKING:
    from ..repository import (
        MinosRepository,
    )
    from .repositories import (
        TransactionRepository,
    )

logger = logging.getLogger(__name__)


class Transaction:
    """TODO"""

    @inject
    def __init__(
        self,
        uuid: Optional[UUID] = None,
        status: TransactionStatus = None,
        event_offset: Optional[int] = None,
        autocommit: bool = True,
        event_repository: MinosRepository = Provide["repository"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
    ):
        if uuid is None:
            uuid = uuid4()
        if status is None:
            status = TransactionStatus.CREATED

        self.uuid = uuid
        self.autocommit = autocommit
        self.status = status
        self.event_offset = event_offset

        self.event_repository = event_repository
        self.transaction_repository = transaction_repository

        self._token = None

    async def __aenter__(self):
        if TRANSACTION_CONTEXT_VAR.get() is not None:
            raise ValueError()
        self._token = TRANSACTION_CONTEXT_VAR.set(self)
        await self.save()
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

        try:
            # noinspection PyProtectedMember
            event_offset = await self.event_repository._commit_transaction(self)
            await self.save(event_offset, status=TransactionStatus.COMMITTED)
        except MinosRepositoryConflictException as exc:
            await self.save(exc.offset, status=TransactionStatus.REJECTED)
            raise exc

    async def save(self, event_offset: Optional[int] = None, status: Optional[TransactionStatus] = None):
        """TODO"""

        if event_offset is not None:
            self.event_offset = event_offset
        if status is not None:
            self.status = status

        await self.transaction_repository.submit(self)

    def __repr__(self):
        return f"{type(self).__name__}(uuid={self.uuid!r}, event_offset={self.event_offset!r},  status={self.status!r})"


class TransactionStatus(str, Enum):
    """TODO"""

    CREATED = "created"
    PENDING = "pending"
    COMMITTED = "committed"
    REJECTED = "rejected"
