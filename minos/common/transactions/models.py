from __future__ import (
    annotations,
)

import logging
from enum import (
    Enum,
)
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Union,
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
        status: Union[str, TransactionStatus] = None,
        event_offset: Optional[int] = None,
        autocommit: bool = True,
        event_repository: MinosRepository = Provide["repository"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
    ):
        if uuid is None:
            uuid = uuid4()
        if status is None:
            status = TransactionStatus.CREATED
        if not isinstance(status, TransactionStatus):
            status = TransactionStatus.value_of(status)

        self.uuid = uuid
        self.autocommit = autocommit
        self.status = status
        self.event_offset = event_offset

        self.event_repository = event_repository
        self.transaction_repository = transaction_repository

        self._token = None

    async def __aenter__(self):
        if self.status in (TransactionStatus.COMMITTED, TransactionStatus.RESERVED, TransactionStatus.REJECTED):
            raise ValueError(
                "Current status is not in "
                f"{(TransactionStatus.COMMITTED, TransactionStatus.RESERVED, TransactionStatus.REJECTED)!r}. "
                f"Obtained: {self.status!r}"
            )

        if TRANSACTION_CONTEXT_VAR.get() is not None:  # FIXME: Future implementations should not have this constraint.
            raise ValueError("Already inside a transaction. Multiple simultaneous transactions are not supported yet!")

        self._token = TRANSACTION_CONTEXT_VAR.set(self)
        await self.save()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        TRANSACTION_CONTEXT_VAR.reset(self._token)

        if self.autocommit and self.status in (TransactionStatus.PENDING, TransactionStatus.RESERVED):
            await self.commit()

    async def reserve(self) -> None:
        """TODO"""
        if self.status != TransactionStatus.PENDING:
            raise ValueError(f"Current status is not {TransactionStatus.PENDING!r}. Obtained: {self.status!r}")

        async with self.event_repository.write_lock():
            # noinspection PyProtectedMember
            committable = await self.event_repository._check_transaction(self)
            # noinspection PyProtectedMember
            event_offset = 1 + await self.event_repository.offset
            if committable:
                await self.save(event_offset=event_offset, status=TransactionStatus.RESERVED)
            else:
                await self.save(event_offset=event_offset, status=TransactionStatus.REJECTED)

    async def reject(self) -> None:
        """TODO"""
        if self.status not in (TransactionStatus.PENDING, TransactionStatus.RESERVED):
            raise ValueError(
                f"Current status is not in {(TransactionStatus.PENDING, TransactionStatus.RESERVED)!r}. "
                f"Obtained: {self.status!r}"
            )

        # noinspection PyProtectedMember
        event_offset = 1 + await self.event_repository.offset
        await self.save(event_offset=event_offset, status=TransactionStatus.REJECTED)

    async def commit(self) -> None:
        """TODO"""

        if self.status not in (TransactionStatus.PENDING, TransactionStatus.RESERVED):
            raise ValueError(
                f"Current status is not in {(TransactionStatus.PENDING, TransactionStatus.RESERVED)!r}. "
                f"Obtained: {self.status!r}"
            )

        try:
            # noinspection PyProtectedMember
            event_offset = 1 + await self.event_repository._commit_transaction(self)
            await self.save(event_offset=event_offset, status=TransactionStatus.COMMITTED)
        except MinosRepositoryConflictException as exc:
            event_offset = 1 + exc.offset
            await self.save(event_offset=event_offset, status=TransactionStatus.REJECTED)
            raise exc

    async def save(self, *, event_offset: Optional[int] = None, status: Optional[TransactionStatus] = None):
        """TODO"""

        if event_offset is not None:
            self.event_offset = event_offset
        if status is not None:
            self.status = status

        await self.transaction_repository.submit(self)

    def __eq__(self, other: Transaction) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (
            self.uuid,
            self.status,
            self.event_offset,
        )

    def __repr__(self):
        return f"{type(self).__name__}(uuid={self.uuid!r}, status={self.status!r}, event_offset={self.event_offset!r})"


class TransactionStatus(str, Enum):
    """TODO"""

    CREATED = "created"
    PENDING = "pending"
    COMMITTED = "committed"
    RESERVED = "reserved"
    REJECTED = "rejected"

    @classmethod
    def value_of(cls, value: str) -> TransactionStatus:
        """Get the status based on its text representation."""
        for item in cls.__members__.values():
            if item.value == value:
                return item
        raise ValueError(f"The given value does not match with any enum items. Obtained {value}")
