from typing import (
    Any,
    AsyncIterator,
)

from minos.common import (
    DatabaseClient,
    DatabaseOperation,
)


class LmdbDatabaseClient(DatabaseClient):
    """TODO"""

    async def _is_valid(self, **kwargs) -> bool:
        pass

    async def _reset(self, **kwargs) -> None:
        pass

    async def _execute(self, operation: DatabaseOperation) -> None:
        pass

    def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        pass
