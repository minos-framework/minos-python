from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    wait_for,
)
from collections.abc import (
    AsyncIterator,
    Hashable,
)
from typing import (
    TYPE_CHECKING,
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
from ..operations import (
    ComposedDatabaseOperation,
    DatabaseOperation,
    DatabaseOperationFactory,
)
from .exceptions import (
    ProgrammingException,
)

if TYPE_CHECKING:
    from ..locks import (
        DatabaseLock,
    )

logger = logging.getLogger(__name__)


class DatabaseClient(ABC, BuildableMixin):
    """Database Client base class."""

    _factories: dict[type[DatabaseOperationFactory], type[DatabaseOperationFactory]]
    _lock: Optional[DatabaseLock]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._lock = None

    @classmethod
    def _from_config(cls, config: Config, name: Optional[str] = None, **kwargs) -> DatabaseClient:
        return super()._from_config(config, **config.get_database_by_name(name), **kwargs)

    async def is_valid(self, **kwargs) -> bool:
        """Check if the instance is valid.

        :return: ``True`` if it is valid or ``False`` otherwise.
        """
        return await self._is_valid(**kwargs)

    async def _is_valid(self, **kwargs) -> bool:
        return True

    async def _destroy(self) -> None:
        await self.reset()
        await super()._destroy()

    async def reset(self, **kwargs) -> None:
        """Reset the current instance status.

        :param kwargs: Additional named parameters.
        :return: This method does not return anything.
        """
        await self._destroy_lock()
        return await self._reset(**kwargs)

    @abstractmethod
    async def _reset(self, **kwargs) -> None:
        raise NotImplementedError

    async def execute(self, operation: DatabaseOperation) -> None:
        """Execute an operation.

        :param operation: The operation to be executed.
        :return: This method does not return anything.
        """
        if not isinstance(operation, DatabaseOperation):
            raise ValueError(f"The operation must be a {DatabaseOperation!r} instance. Obtained: {operation!r}")

        if operation.lock is not None:
            await self._create_lock(operation.lock)

        if isinstance(operation, ComposedDatabaseOperation):
            await wait_for(self._execute_composed(operation), operation.timeout)
        else:
            await wait_for(self._execute(operation), operation.timeout)

    async def _execute_composed(self, operation: ComposedDatabaseOperation) -> None:
        for op in operation.operations:
            await self.execute(op)

    @abstractmethod
    async def _execute(self, operation: DatabaseOperation) -> None:
        raise NotImplementedError

    async def _create_lock(self, lock: Hashable, *args, **kwargs):
        if self._lock is not None and self._lock.key == lock:
            return
        await self._destroy_lock()

        from ..locks import (
            DatabaseLock,
        )

        self._lock = DatabaseLock(self, lock, *args, **kwargs)
        await self._lock.acquire()

    async def _destroy_lock(self):
        if self._lock is not None:
            logger.debug(f"Destroying {self._lock!r}...")
            await self._lock.release()
            self._lock = None

    @property
    def lock(self) -> Optional[DatabaseLock]:
        """Get the lock.

        :return: A ``DatabaseLock`` instance.
        """
        return self._lock

    async def fetch_one(self) -> Any:
        """Fetch one value.

        :return: This method does not return anything.
        """
        try:
            return await self.fetch_all().__anext__()
        except StopAsyncIteration:
            raise ProgrammingException("There are not any value to be fetched.")

    def fetch_all(self) -> AsyncIterator[Any]:
        """Fetch all values with an asynchronous iterator.

        :return: This method does not return anything.
        """
        return self._fetch_all()

    @abstractmethod
    def _fetch_all(self, *args, **kwargs) -> AsyncIterator[Any]:
        raise NotImplementedError

    @classmethod
    def set_factory(cls, base: type[DatabaseOperationFactory], impl: type[DatabaseOperationFactory]) -> None:
        """Register an operation factory implementation for an operation factory interface.

        :param base: The operation factory interface.
        :param impl: The operation factory implementation.
        :return: This method does not return anything.
        """
        if not issubclass(base, DatabaseOperationFactory):
            raise ValueError(f"{base!r} must be a subclass of {DatabaseOperationFactory!r}")

        if not issubclass(impl, base):
            raise ValueError(f"{impl!r} must be a subclass of {base!r}")

        if not hasattr(cls, "_factories"):
            cls._factories = dict()

        cls._factories[base] = impl

    @classmethod
    def get_factory(cls, base: type[DatabaseOperationFactory]) -> DatabaseOperationFactory:
        """Get an operation factory implementation for an operation factory interface.

        :param base: The operation factory interface.
        :return: The operation factory implementation.
        """
        if not hasattr(cls, "_factories") or base not in cls._factories:
            raise ValueError(f"{cls!r} does not contain any registered factory implementation for {base!r}")

        return cls._factories[base]()


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
