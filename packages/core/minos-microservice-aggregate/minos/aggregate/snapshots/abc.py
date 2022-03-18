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
    Injectable,
    SetupMixin,
)

from ..queries import (
    _TRUE_CONDITION,
    _Condition,
    _Ordering,
)
from ..transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
)

if TYPE_CHECKING:
    from ..entities import (
        RootEntity,
    )


@Injectable("snapshot_repository")
class SnapshotRepository(ABC, SetupMixin):
    """Base Snapshot class.

    The snapshot provides a direct accessor to the ``RootEntity`` instances stored as events by the event repository
    class.
    """

    async def get(self, name: str, uuid: UUID, transaction: Optional[TransactionEntry] = None, **kwargs) -> RootEntity:
        """Get a ``RootEntity`` instance from its identifier.

        :param name: Class name of the ``RootEntity``.
        :param uuid: Identifier of the ``RootEntity``.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param kwargs: Additional named arguments.
        :return: The ``RootEntity`` instance.
        """
        if transaction is None:
            transaction = TRANSACTION_CONTEXT_VAR.get()

        await self.synchronize(**kwargs)

        return await self._get(name=name, uuid=uuid, transaction=transaction, **kwargs)

    @abstractmethod
    async def _get(self, *args, **kwargs) -> RootEntity:
        raise NotImplementedError

    def get_all(
        self,
        name: str,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        **kwargs,
    ) -> AsyncIterator[RootEntity]:
        """Get all ``RootEntity`` instances.

        :param name: Class name of the ``RootEntity``.
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
        :return: An asynchronous iterator that containing the ``RootEntity`` instances.
        """
        return self.find(
            name,
            _TRUE_CONDITION,
            ordering=ordering,
            limit=limit,
            streaming_mode=streaming_mode,
            transaction=transaction,
            **kwargs,
        )

    async def find(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        **kwargs,
    ) -> AsyncIterator[RootEntity]:
        """Find a collection of ``RootEntity`` instances based on a ``Condition``.

        :param name: Class name of the ``RootEntity``.
        :param condition: The condition that must be satisfied by the ``RootEntity`` instances.
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
        :return: An asynchronous iterator that containing the ``RootEntity`` instances.
        """
        if transaction is None:
            transaction = TRANSACTION_CONTEXT_VAR.get()

        await self.synchronize(**kwargs)

        iterable = self._find(
            name=name,
            condition=condition,
            ordering=ordering,
            limit=limit,
            streaming_mode=streaming_mode,
            transaction=transaction,
            **kwargs,
        )

        async for instance in iterable:
            yield instance

    @abstractmethod
    def _find(self, *args, **kwargs) -> AsyncIterator[RootEntity]:
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
