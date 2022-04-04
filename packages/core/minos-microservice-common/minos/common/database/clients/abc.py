from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
)
from typing import (
    Any,
    Optional,
)

from ...setup import (
    SetupMixin,
)


class DatabaseClient(ABC, SetupMixin):
    """TODO"""

    @abstractmethod
    async def is_valid(self, **kwargs) -> bool:
        """TODO"""

    @abstractmethod
    async def submit_query(
        self, operation: Any, parameters: Any = None, *, timeout: Optional[float] = None, lock: Any = None, **kwargs
    ) -> None:
        """TODO"""

    @abstractmethod
    async def submit_query_and_fetchone(
        self,
        operation: Any,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
        lock: Optional[int] = None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> Any:
        """TODO"""

    @abstractmethod
    def submit_query_and_iter(
        self,
        operation: Any,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
        lock: Optional[int] = None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> AsyncIterator[Any]:
        """TODO"""


class UnableToConnectException(Exception):
    """TODO"""
