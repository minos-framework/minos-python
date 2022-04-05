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
    def _from_config(cls, config: Config, name: Optional[str] = None, **kwargs) -> DatabaseClient:
        return super()._from_config(config, **config.get_database_by_name(name), **kwargs)

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

    def with_name(self, name: str) -> DatabaseClientBuilder:
        """TODO"""
        self.kwargs["name"] = name
        return self

    def with_config(self, config: Config) -> DatabaseClientBuilder:
        """TODO"""
        database_config = config.get_database_by_name(self.kwargs.get("name"))
        self.kwargs |= database_config
        return self


DatabaseClient.set_builder(DatabaseClientBuilder)


class UnableToConnectException(Exception):
    """TODO"""
