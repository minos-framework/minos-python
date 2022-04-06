from collections.abc import (
    Hashable,
)
from typing import (
    Any,
    Optional,
    Union,
)

from psycopg2.sql import (
    Composable,
)

from .abc import (
    DatabaseOperation,
)


class AiopgDatabaseOperation(DatabaseOperation):
    """TODO"""

    def __init__(
        self, query: Union[str, Composable], parameters: dict[str, Any] = None, lock: Optional[Hashable] = None
    ):
        if parameters is None:
            parameters = dict()
        self.query = query
        self.parameters = parameters
        self.lock = lock
