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
    from .deltas import (
        Delta,
    )
    from .entities import (
        Entity,
    )


class AggregateException(MinosException):
    """Base Aggregate module exception"""


class DeltaRepositoryException(AggregateException):
    """Base delta repository exception."""


class DeltaRepositoryConflictException(DeltaRepositoryException):
    """Exception to be raised when some ``DeltaEntry`` raises a conflict."""

    def __init__(self, error_message: str, offset: int):
        super().__init__(error_message)
        self.offset = offset


class SnapshotRepositoryException(AggregateException):
    """Base snapshot exception."""


class SnapshotRepositoryConflictException(SnapshotRepositoryException):
    """Exception to be raised when current version is newer than the one to be processed."""

    def __init__(self, previous: Entity, delta: Delta):
        self.previous = previous
        self.delta = delta
        super().__init__(
            f"Version for {repr(previous.classname)} root entity must be "
            f"greater than {previous.version}. Obtained: {delta.version}"
        )


class NotFoundException(SnapshotRepositoryException):
    """Exception to be raised when a ``Entity`` is not found on the repository."""


class AlreadyDeletedException(SnapshotRepositoryException):
    """Exception to be raised when a ``Entity`` is already deleted from the repository."""


class ValueObjectException(AggregateException):
    """If an attribute of an immutable class is modified, this exception will be raised"""


class RefException(AggregateException):
    """Exception to be raised when some reference can not be resolved."""
