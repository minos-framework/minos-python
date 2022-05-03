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
    Any,
    Iterable,
    Optional,
    Union,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    NULL_UUID,
    Inject,
    NotProvidedException,
)

from ..exceptions import (
    EventRepositoryConflictException,
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
        "destination_uuid",
        "updated_at",
        "_autocommit",
        "_event_repository",
        "_transaction_repository",
        "_token",
    )

    def __init__(
        self,
        uuid: Optional[UUID] = None,
        status: Union[str, TransactionStatus] = None,
        event_offset: Optional[int] = None,
        destination_uuid: Optional[UUID] = None,
        updated_at: Optional[datetime] = None,
        autocommit: bool = True,
        event_repository: Optional[EventRepository] = None,
        transaction_repository: Optional[TransactionRepository] = None,
    ):

        if event_repository is None:
            from ..events import (
                EventRepository,
            )

            with suppress(NotProvidedException):
                event_repository = Inject.resolve(EventRepository)

        if transaction_repository is None:
            from .repositories import (
                TransactionRepository,
            )

            with suppress(NotProvidedException):
                transaction_repository = Inject.resolve(TransactionRepository)

        if uuid is None:
            uuid = uuid4()
        if status is None:
            status = TransactionStatus.PENDING
        if not isinstance(status, TransactionStatus):
            status = TransactionStatus.value_of(status)

        if destination_uuid is None:
            outer = TRANSACTION_CONTEXT_VAR.get()
            outer_uuid = getattr(outer, "uuid", NULL_UUID)
            destination_uuid = outer_uuid

        self.uuid = uuid
        self.status = status
        self.event_offset = event_offset
        self.destination_uuid = destination_uuid
        self.updated_at = updated_at

        self._autocommit = autocommit
        self._event_repository = event_repository
        self._transaction_repository = transaction_repository

        self._token = None

    async def __aenter__(self):
        if self.status != TransactionStatus.PENDING:
            raise ValueError(f"Current status is not {TransactionStatus.PENDING!r}. Obtained: {self.status!r}")

        outer = TRANSACTION_CONTEXT_VAR.get()
        outer_uuid = getattr(outer, "uuid", NULL_UUID)
        if outer_uuid != self.destination_uuid:
            raise ValueError(f"{self!r} requires to be run on top of {outer!r}")

        self._token = TRANSACTION_CONTEXT_VAR.set(self)
        await self.save()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        TRANSACTION_CONTEXT_VAR.reset(self._token)

        if self._autocommit and self.status == TransactionStatus.PENDING:
            await self.commit()

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
            new = EventEntry.from_another(entry, transaction_uuid=self.destination_uuid)
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
                    raise EventRepositoryConflictException(f"{self!r} could not be reserved!", event_offset)

    async def validate(self) -> bool:
        """Check if the transaction is committable.

        :return: ``True`` if the transaction is valid or ``False`` otherwise.
        """
        with suppress(StopAsyncIteration):
            iterable = self._transaction_repository.select(
                uuid=self.destination_uuid,
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
            if entry.uuid in entries and entry.version < entries[entry.uuid]:
                continue
            entries[entry.uuid] = entry.version

        transaction_uuids = set()
        for uuid, version in entries.items():
            async for entry in self._event_repository.select(uuid=uuid, version=version):
                if entry.transaction_uuid == self.destination_uuid:
                    return False
                if entry.transaction_uuid != self.uuid:
                    transaction_uuids.add(entry.transaction_uuid)

        if len(transaction_uuids):
            with suppress(StopAsyncIteration):
                iterable = self._transaction_repository.select(
                    destination_uuid=self.destination_uuid,
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

    @property
    async def uuids(self) -> tuple[UUID, ...]:
        """Get the sequence of transaction identifiers, from the outer one (``NULL_UUID``) to the one related with self.

        :return: A tuple of ``UUID`` values.
        """
        uuids = []
        current = self

        while current is not None:
            uuids.append(current.uuid)
            current = await current.destination

        # noinspection PyRedundantParentheses
        return (NULL_UUID, *uuids[::-1])

    @property
    async def destination(self) -> Optional[TransactionEntry]:
        """Get the destination transaction if there is anyone, otherwise ``None`` is returned.

        :return: A ``TransactionEntry`` or ``None``.
        """

        if self.destination_uuid == NULL_UUID:
            return None

        destination = getattr(self._token, "old_value", Token.MISSING)

        if destination == Token.MISSING:
            destination = await self._transaction_repository.get(uuid=self.destination_uuid)

        return destination

    def __eq__(self, other: TransactionEntry) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (
            self.uuid,
            self.status,
            self.event_offset,
            self.destination_uuid,
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}(uuid={self.uuid!r}, status={self.status!r}, event_offset={self.event_offset!r}, "
            f"destination_uuid={self.destination_uuid!r}, updated_at={self.updated_at!r})"
        )

    def as_raw(self) -> dict[str, Any]:
        """Get a raw representation of the instance.

        :return: A dictionary in which the keys are attribute names and values the attribute contents.
        """
        return {
            "uuid": self.uuid,
            "status": self.status,
            "event_offset": self.event_offset,
            "destination_uuid": self.destination_uuid,
            "updated_at": self.updated_at,
        }


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
