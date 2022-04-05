from __future__ import (
    annotations,
)

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
    Optional,
)

from ...builders import (
    BuildableMixin,
    Builder,
)
from ...config import (
    Config,
)


class DatabaseClient(ABC, BuildableMixin):
    """TODO"""

    @classmethod
    def _from_config(cls, config: Config, key: Optional[str] = None, **kwargs) -> DatabaseClient:
        return super()._from_config(config, **config.get_database_by_name(key), **kwargs)

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


class DatabaseClientBuilder(Builder[DatabaseClient]):
    """TODO"""

    def with_key(self, key) -> DatabaseClientBuilder:
        """TODO"""
        self.kwargs["key"] = key
        return self

    def with_config(self, config: Config) -> DatabaseClientBuilder:
        """TODO"""
        database_config = config.get_database_by_name(self.kwargs.get("key"))
        self.kwargs |= database_config
        return self


DatabaseClient.set_builder(DatabaseClientBuilder)


class UnableToConnectException(Exception):
    """TODO"""
