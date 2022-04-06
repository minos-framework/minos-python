from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)


class DatabaseOperation(ABC):
    """TODO"""


class ComposedDatabaseOperation(DatabaseOperation):
    """TODO"""

    def __init__(self, operations: Iterable[DatabaseOperation]):
        super().__init__()
        self.operations = operations
