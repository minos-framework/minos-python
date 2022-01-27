from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Awaitable,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    MinosSetup,
)

from ..queries import (
    _Condition,
    _Ordering,
)
from ..transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
)

if TYPE_CHECKING:
    from ..models import (
        Aggregate,
    )


class SnapshotRepository(ABC, MinosSetup):
    """Base Snapshot class.

    The snapshot provides a direct accessor to the aggregate instances stored as events by the event repository class.
    """

    async def get(
        self, aggregate_name: str, uuid: UUID, transaction: Optional[TransactionEntry] = None, **kwargs
    ) -> Aggregate:
        """Get an aggregate instance from its identifier.

        :param aggregate_name: Class name of the ``Aggregate``.
        :param uuid: Identifier of the ``Aggregate``.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param kwargs: Additional named arguments.
        :return: The ``Aggregate`` instance.
        """
        if transaction is None:
            transaction = TRANSACTION_CONTEXT_VAR.get()

        await self.synchronize(**kwargs)

        return await self._get(aggregate_name=aggregate_name, uuid=uuid, transaction=transaction, **kwargs)

    @abstractmethod
    async def _get(self, *args, **kwargs) -> Aggregate:
        raise NotImplementedError

    async def find(
        self,
        aggregate_name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        **kwargs,
    ) -> AsyncIterator[Aggregate]:
        """Find a collection of ``Aggregate`` instances based on a ``Condition``.

        :param aggregate_name: Class name of the ``Aggregate``.
        :param condition: The condition that must be satisfied by the ``Aggregate`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param streaming_mode: If ``True`` return the values in streaming directly from the database (keep an open
            database connection), otherwise preloads the full set of values on memory and then retrieves them.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``Aggregate`` instances.
        """
        if transaction is None:
            transaction = TRANSACTION_CONTEXT_VAR.get()

        await self.synchronize(**kwargs)

        iterable = self._find(
            aggregate_name=aggregate_name,
            condition=condition,
            ordering=ordering,
            limit=limit,
            streaming_mode=streaming_mode,
            transaction=transaction,
            **kwargs,
        )

        async for aggregate in iterable:
            yield aggregate

    @abstractmethod
    def _find(self, *args, **kwargs) -> AsyncIterator[Aggregate]:
        raise NotImplementedError

    def synchronize(self, **kwargs) -> Awaitable[None]:
        """Synchronize the snapshot to the latest available version.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return self._synchronize(**kwargs)

    @abstractmethod
    async def _synchronize(self, **kwargs) -> None:
        raise NotImplementedError
