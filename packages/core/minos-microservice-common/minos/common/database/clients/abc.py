from __future__ import (
    annotations,
)

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

from ...builders import (
    BuildableMixin,
    Builder,
)
from ...config import (
    Config,
)


class DatabaseClient(ABC, BuildableMixin):
    """Database Client base class."""

    @classmethod
    def _from_config(cls, config: Config, name: Optional[str] = None, **kwargs) -> DatabaseClient:
        return super()._from_config(config, **config.get_database_by_name(name), **kwargs)

    async def is_valid(self, **kwargs) -> bool:
        """Check if the instance is valid.

        :return: ``True`` if it is valid or ``False`` otherwise.
        """
        return await self._is_valid(**kwargs)

    @abstractmethod
    async def _is_valid(self, **kwargs) -> bool:
        raise NotImplementedError

    async def reset(self, **kwargs) -> None:
        """Reset the current instance status.

        :param kwargs: Additional named parameters.
        :return: This method does not return anything.
        """
        return await self._reset(**kwargs)

    @abstractmethod
    async def _reset(self, **kwargs) -> None:
        raise NotImplementedError

    async def execute(self, *args, **kwargs) -> None:
        """Execute an operation.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        await self._execute(*args, **kwargs)

    @abstractmethod
    async def _execute(self, *args, **kwargs) -> None:
        raise NotImplementedError

    async def fetch_one(self, *args, **kwargs) -> Any:
        """Fetch one value.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return await self.fetch_all(*args, **kwargs).__anext__()

    def fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        """Fetch all values with an asynchronous iterator.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return self._fetch_all(*args, **kwargs)

    @abstractmethod
    def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        raise NotImplementedError


class DatabaseClientBuilder(Builder[DatabaseClient]):
    """Database Client Builder class."""

    def with_name(self, name: str) -> DatabaseClientBuilder:
        """Set name.

        :param name: The name to be added.
        :return: This method return the builder instance.
        """
        self.kwargs["name"] = name
        return self

    def with_config(self, config: Config) -> DatabaseClientBuilder:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        database_config = config.get_database_by_name(self.kwargs.get("name"))
        self.kwargs |= database_config
        return self


DatabaseClient.set_builder(DatabaseClientBuilder)
