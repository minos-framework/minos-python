from enum import (
    Enum,
)
from typing import (
    Any,
    Optional,
)

from minos.common import (
    DatabaseOperation,
)


class LmdbDatabaseOperationType(str, Enum):
    """TODO"""

    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"


class LmdbDatabaseOperation(DatabaseOperation):
    """TODO"""

    def __init__(
        self, type_: LmdbDatabaseOperationType, table: str, key: str, value: Optional[Any] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.type_ = type_
        self.table = table
        self.key = key
        self.value = value
