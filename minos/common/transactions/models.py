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

from .contextvars import (
    TRANSACTION_CONTEXT_VAR,
)

if TYPE_CHECKING:
    from ..repository import (
        MinosRepository,
    )
    from .pg import (
        PostgreSqlTransactionRepository,
    )

logger = logging.getLogger(__name__)


class Transaction:
    """TODO"""

    @inject
    def __init__(
        self,
        event_repository: MinosRepository = Provide["transaction_repository"],
        transaction_repository: PostgreSqlTransactionRepository = Provide["transaction_repository"],
        uuid: Optional[UUID] = None,
        autocommit: bool = True,
        status: TransactionStatus = None,
    ):
        if uuid is None:
            uuid = uuid4()
        if status is None:
            status = TransactionStatus.CREATED

        self.event_repository = event_repository
        self.transaction_repository = transaction_repository

        self.uuid = uuid
        self.autocommit = autocommit
        self.status = status
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
            await self.event_repository._commit_transaction(self.uuid)
            self.status = TransactionStatus.COMMITTED
        except Exception as exc:
            self.status = TransactionStatus.REJECTED
            raise exc
        finally:
            await self.save()

    async def save(self):
        """TODO"""

        if not isinstance(self.transaction_repository, PostgreSqlTransactionRepository):
            logger.warning("TransactionRepository is not configured...")
            return
        await self.transaction_repository.submit(self)

    def __repr__(self):
        return f"{type(self).__name__}(uuid={self.uuid!r}, status={self.status!r})"


class TransactionStatus(str, Enum):
    """TODO"""

    CREATED = "created"
    PENDING = "pending"
    COMMITTED = "committed"
    REJECTED = "rejected"
