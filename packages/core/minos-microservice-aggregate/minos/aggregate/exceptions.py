from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
)

from minos.common import (
    MinosException,
)

if TYPE_CHECKING:
    from .entities import (
        RootEntity,
    )
    from .events import (
        Event,
    )


class AggregateException(MinosException):
    """Base Aggregate module exception"""


class EventRepositoryException(AggregateException):
    """Base event repository exception."""


class EventRepositoryConflictException(EventRepositoryException):
    """Exception to be raised when some ``EventEntry`` raises a conflict."""

    def __init__(self, error_message: str, offset: int):
        super().__init__(error_message)
        self.offset = offset


class TransactionRepositoryException(AggregateException):
    """Base transaction repository exception."""


class TransactionRepositoryConflictException(TransactionRepositoryException):
    """Exception to be raised when a transaction has invalid status."""


class TransactionNotFoundException(TransactionRepositoryException):
    """Exception to be raised when some transaction is not found on the repository."""


class SnapshotRepositoryException(AggregateException):
    """Base snapshot exception."""


class SnapshotRepositoryConflictException(SnapshotRepositoryException):
    """Exception to be raised when current version is newer than the one to be processed."""

    def __init__(self, previous: RootEntity, event: Event):
        self.previous = previous
        self.event = event
        super().__init__(
            f"Version for {repr(previous.classname)} root entity must be "
            f"greater than {previous.version}. Obtained: {event.version}"
        )


class NotFoundException(SnapshotRepositoryException):
    """Exception to be raised when a ``RootEntity`` is not found on the repository."""


class AlreadyDeletedException(SnapshotRepositoryException):
    """Exception to be raised when a ``RootEntity`` is already deleted from the repository."""


class ValueObjectException(AggregateException):
    """If an attribute of an immutable class is modified, this exception will be raised"""


class RefException(AggregateException):
    """Exception to be raised when some reference can not be resolved."""
