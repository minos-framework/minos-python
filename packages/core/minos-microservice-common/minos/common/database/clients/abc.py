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
    """Database Client base class."""

    @classmethod
    def _from_config(cls, config: Config, name: Optional[str] = None, **kwargs) -> DatabaseClient:
        return super()._from_config(config, **config.get_database_by_name(name), **kwargs)

    @abstractmethod
    async def is_valid(self, **kwargs) -> bool:
        """Check if the instance is valid.

        :return: ``True`` if it is valid or ``False`` otherwise.
        """

    async def reset(self, **kwargs) -> None:
        """Reset the current instance status.

        :param kwargs: Additional named parameters.
        :return: This method does not return anything.
        """

    @abstractmethod
    async def execute(self, *args, **kwargs) -> None:
        """Execute an operation.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """

    async def fetch_one(self, *args, **kwargs) -> Any:
        """Fetch one value.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return await self.fetch_all(*args, **kwargs).__anext__()

    @abstractmethod
    def fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        """Fetch all values with an asynchronous iterator.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """

    @property
    @abstractmethod
    def notifications(self) -> Queue:
        """Get the notifications queue.

        :return: A ``Queue`` instance.
        """


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
