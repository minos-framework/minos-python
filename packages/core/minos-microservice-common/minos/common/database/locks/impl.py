from __future__ import (
    annotations,
)

from collections.abc import (
    Hashable,
)
from typing import (
    Optional,
)

from ...locks import (
    Lock,
)
from ..clients import (
    DatabaseClient,
)
from .factories import (
    LockDatabaseOperationFactory,
)


class DatabaseLock(Lock):
    """Database Lock class."""

    def __init__(
        self,
        client: DatabaseClient,
        key: Hashable,
        *args,
        operation_factory: Optional[LockDatabaseOperationFactory] = None,
        **kwargs,
    ):
        super().__init__(key, *args, **kwargs)
        if operation_factory is None:
            operation_factory = client.get_factory(LockDatabaseOperationFactory)

        self.client = client
        self.operation_factory = operation_factory

    async def acquire(self) -> None:
        """Acquire the lock.

        :return: This method does not return anything.
        """
        operation = self.operation_factory.build_acquire(self.hashed_key)
        await self.client.execute(operation)

    async def release(self) -> None:
        """Release the lock.

        :return: This method does not return anything.
        """
        operation = self.operation_factory.build_release(self.hashed_key)
        await self.client.execute(operation)
