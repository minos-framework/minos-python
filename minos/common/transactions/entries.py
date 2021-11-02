from __future__ import (
    annotations,
)

import logging
from contextlib import (
    suppress,
)
from contextvars import (
    Token,
)
from datetime import (
    datetime,
)
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
from ..uuid import (
    NULL_UUID,
)
from .contextvars import (
    TRANSACTION_CONTEXT_VAR,
)

if TYPE_CHECKING:
    from ..events import (
        EventRepository,
    )
    from .repositories import (
        TransactionRepository,
    )

logger = logging.getLogger(__name__)


class TransactionEntry:
    """Transaction Entry class."""

    __slots__ = (
        "uuid",
        "status",
        "event_offset",
        "destination",
        "updated_at",
        "_autocommit",
        "_event_repository",
        "_transaction_repository",
        "_token",
    )

    @inject
    def __init__(
        self,
        uuid: Optional[UUID] = None,
        status: Union[str, TransactionStatus] = None,
        event_offset: Optional[int] = None,
        destination: Optional[UUID] = None,
        updated_at: Optional[datetime] = None,
        autocommit: bool = True,
        event_repository: EventRepository = Provide["event_repository"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
    ):
        if uuid is None:
            uuid = uuid4()
        if status is None:
            status = TransactionStatus.PENDING
        if not isinstance(status, TransactionStatus):
            status = TransactionStatus.value_of(status)
        if destination is None:
            outer = TRANSACTION_CONTEXT_VAR.get()
            if outer is None:
                destination = NULL_UUID
            else:
                destination = outer.uuid

        self.uuid = uuid
        self.status = status
        self.event_offset = event_offset
        self.destination = destination
        self.updated_at = updated_at

        self._autocommit = autocommit
        self._event_repository = event_repository
        self._transaction_repository = transaction_repository

        self._token = None

    async def __aenter__(self):
        if self.status != TransactionStatus.PENDING:
            raise ValueError(f"Current status is not {TransactionStatus.PENDING!r}. Obtained: {self.status!r}")

        destination = TRANSACTION_CONTEXT_VAR.get()
        destination_uuid = getattr(destination, "uuid", NULL_UUID)

        if destination_uuid != self.destination:
            raise ValueError(
                "Already inside a transaction. Multiple simultaneous transactions are not supported yet!"
            )

        self._token = TRANSACTION_CONTEXT_VAR.set(self)
        await self.save()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        TRANSACTION_CONTEXT_VAR.reset(self._token)

        if self._autocommit and self.status == TransactionStatus.PENDING:
            await self.commit()

    @property
    async def uuids(self) -> tuple[UUID, ...]:
        """TODO"""
        uuids = []
        current = self
        while True:
            uuids.append(current.uuid)
            if current._token is not None:
                destination = current._token.old_value
            else:
                destination = Token.MISSING
            if destination == Token.MISSING:
                if current.destination == NULL_UUID:
                    uuids.append(NULL_UUID)
                    break
                else:
                    destination = await self._transaction_repository.select(uuid=current.destination).__anext__()

            if destination.uuid != current.destination:
                raise ValueError()
            current = destination

        return tuple(uuids[::-1])

    async def commit(self) -> None:
        """Commit transaction changes.

        :return: This method does not return anything.
        """

        if self.status == TransactionStatus.PENDING:
            await self.reserve()

        if self.status != TransactionStatus.RESERVED:
            raise ValueError(f"Current status is not {TransactionStatus.RESERVED!r}. Obtained: {self.status!r}")

        async with self._transaction_repository.write_lock():
            await self.save(status=TransactionStatus.COMMITTING)
            await self._commit()
            event_offset = 1 + await self._event_repository.offset
            await self.save(event_offset=event_offset, status=TransactionStatus.COMMITTED)

    async def _commit(self) -> None:
        from ..events import (
            EventEntry,
        )

        async for entry in self._event_repository.select(transaction_uuid=self.uuid):
            new = EventEntry.from_another(entry, transaction_uuid=self.destination)
            await self._event_repository.submit(new, transaction_uuid_ne=self.uuid)

    async def reserve(self) -> None:
        """Reserve transaction changes to be ensured that they can be applied.

        :return: This method does not return anything.
        """
        if self.status != TransactionStatus.PENDING:
            raise ValueError(f"Current status is not {TransactionStatus.PENDING!r}. Obtained: {self.status!r}")

        async with self._transaction_repository.write_lock():
            async with self._event_repository.write_lock():
                await self.save(status=TransactionStatus.RESERVING)

                committable = await self.validate()

                status = TransactionStatus.RESERVED if committable else TransactionStatus.REJECTED
                event_offset = 1 + await self._event_repository.offset
                await self.save(event_offset=event_offset, status=status)
                if not committable:
                    raise MinosRepositoryConflictException(f"{self!r} could not be reserved!", event_offset)

    async def validate(self) -> bool:
        """Check if the transaction is committable.

        :return: ``True`` if the transaction is valid or ``False`` otherwise.
        """
        with suppress(StopAsyncIteration):
            iterable = self._transaction_repository.select(
                uuid=self.destination,
                status_in=(
                    TransactionStatus.RESERVING,
                    TransactionStatus.RESERVED,
                    TransactionStatus.COMMITTING,
                    TransactionStatus.COMMITTED,
                    TransactionStatus.REJECTED,
                ),
            )
            await iterable.__anext__()  # Will raise a `StopAsyncIteration` exception if not any item.

            return False

        entries = dict()
        async for entry in self._event_repository.select(transaction_uuid=self.uuid):
            if entry.aggregate_uuid in entries and entry.version < entries[entry.aggregate_uuid]:
                continue
            entries[entry.aggregate_uuid] = entry.version

        transaction_uuids = set()
        for aggregate_uuid, version in entries.items():
            async for entry in self._event_repository.select(aggregate_uuid=aggregate_uuid, version=version):
                if entry.transaction_uuid == self.destination:
                    return False
                if entry.transaction_uuid != self.uuid:
                    transaction_uuids.add(entry.transaction_uuid)

        if len(transaction_uuids):
            with suppress(StopAsyncIteration):
                iterable = self._transaction_repository.select(
                    destination=self.destination,
                    uuid_in=tuple(transaction_uuids),
                    status_in=(
                        TransactionStatus.RESERVING,
                        TransactionStatus.RESERVED,
                        TransactionStatus.COMMITTING,
                        TransactionStatus.COMMITTED,
                    ),
                )
                await iterable.__anext__()  # Will raise a `StopAsyncIteration` exception if not any item.

                return False

        return True

    async def reject(self) -> None:
        """Reject transaction changes.

        :return: This method does not return anything.
        """
        if self.status not in (TransactionStatus.PENDING, TransactionStatus.RESERVED):
            raise ValueError(
                f"Current status is not in {(TransactionStatus.PENDING, TransactionStatus.RESERVED)!r}. "
                f"Obtained: {self.status!r}"
            )

        async with self._transaction_repository.write_lock():
            event_offset = 1 + await self._event_repository.offset
            await self.save(event_offset=event_offset, status=TransactionStatus.REJECTED)

    async def save(self, *, event_offset: Optional[int] = None, status: Optional[TransactionStatus] = None) -> None:
        """Saves the transaction into the repository.

        :param event_offset: The event offset.
        :param status: The status.
        :return: This method does not return anything.
        """

        if event_offset is not None:
            self.event_offset = event_offset
        if status is not None:
            self.status = status

        await self._transaction_repository.submit(self)

    def __eq__(self, other: TransactionEntry) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (
            self.uuid,
            self.status,
            self.event_offset,
            self.destination,
            self.updated_at,
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}(uuid={self.uuid!r}, status={self.status!r}, event_offset={self.event_offset!r}, "
            f"destination={self.destination!r}, updated_at={self.updated_at!r})"
        )


class TransactionStatus(str, Enum):
    """Transaction Status Enum."""

    PENDING = "pending"
    RESERVING = "reserving"
    RESERVED = "reserved"
    COMMITTING = "committing"
    COMMITTED = "committed"
    REJECTED = "rejected"

    @classmethod
    def value_of(cls, value: str) -> TransactionStatus:
        """Get the status based on its text representation."""
        for item in cls.__members__.values():
            if item.value == value:
                return item
        raise ValueError(f"The given value does not match with any enum items. Obtained {value}")
