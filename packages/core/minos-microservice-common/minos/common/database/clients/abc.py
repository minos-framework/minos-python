from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    Queue,
)
from collections.abc import (
    AsyncIterator,
)
from typing import (
    Any,
)

from ...setup import (
    SetupMixin,
)


class DatabaseClient(ABC, SetupMixin):
    """TODO"""

    @abstractmethod
    async def is_valid(self, **kwargs) -> bool:
        """TODO"""

    async def reset(self, **kwargs) -> None:
        """TODO"""

    @property
    @abstractmethod
    def notifications(self) -> Queue:
        """TODO"""

    @abstractmethod
    async def execute(self, *args, **kwargs) -> None:
        """TODO"""

    async def fetch_one(self, *args, **kwargs) -> Any:
        return await self.fetch_all(*args, **kwargs).__anext__()

    @abstractmethod
    def fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        """TODO"""


class UnableToConnectException(Exception):
    """TODO"""
