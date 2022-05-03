from collections.abc import (
    Iterable,
)
from typing import (
    Any,
    Optional,
)

from ...database import (
    DatabaseOperation,
)


class MockedDatabaseOperation(DatabaseOperation):
    """For testing purposes"""

    def __init__(self, content: str, response: Optional[Iterable[Any]] = tuple(), *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content = content
        self.response = tuple(response)
