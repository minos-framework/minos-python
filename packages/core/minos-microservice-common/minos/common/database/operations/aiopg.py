from typing import (
    Any,
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

    def __init__(self, query: Union[str, Composable], parameters: dict[str, Any] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if parameters is None:
            parameters = dict()
        self.query = query
        self.parameters = parameters
