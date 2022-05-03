from collections.abc import (
    AsyncIterator,
)
from typing import (
    Any,
)

from ...database import (
    DatabaseClient,
)
from .operations import (
    MockedDatabaseOperation,
)


class MockedDatabaseClient(DatabaseClient):
    """For testing purposes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = kwargs
        self._response = tuple()

    async def _reset(self, **kwargs) -> None:
        """For testing purposes"""
        self._response = tuple()

    async def _execute(self, operation: MockedDatabaseOperation) -> None:
        """For testing purposes"""
        self._response = operation.response

    async def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        """For testing purposes"""
        for value in self._response:
            yield value
