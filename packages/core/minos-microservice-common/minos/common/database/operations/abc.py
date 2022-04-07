from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from collections.abc import (
    Hashable,
    Iterable,
)
from typing import (
    Optional,
)


class DatabaseOperation(ABC):
    """Database Operation base class."""

    def __init__(self, *args, lock: Optional[Hashable] = None, timeout: Optional[float] = None, **kwargs):
        self.lock = lock
        self.timeout = timeout


class ComposedDatabaseOperation(DatabaseOperation):
    """Composed Database Operation class."""

    def __init__(self, operations: Iterable[DatabaseOperation], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operations = tuple(operations)


class DatabaseOperationFactory(ABC):
    """Database Operation Factory base class."""
