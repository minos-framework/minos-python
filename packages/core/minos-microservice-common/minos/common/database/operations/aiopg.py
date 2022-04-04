from typing import (
    Any,
)

from .abc import (
    DatabaseOperation,
)


class AiopgDatabaseOperation(DatabaseOperation):
    """TODO"""

    def __init__(self, query: str, arguments: dict[str, Any]):
        self.query = query
        self.arguments = arguments
