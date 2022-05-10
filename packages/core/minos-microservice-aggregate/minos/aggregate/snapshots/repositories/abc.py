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
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    Injectable,
    ModelType,
    SetupMixin,
    classname,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
)

from ...exceptions import (
    NotFoundException,
)
from ...queries import (
    _TRUE_CONDITION,
    _Condition,
    _EqualCondition,
    _Ordering,
)
from ..entries import (
    SnapshotEntry,
)

if TYPE_CHECKING:
    from ...entities import (
        Entity,
    )


@Injectable("snapshot_repository")
class SnapshotRepository(ABC, SetupMixin):
    """Base Snapshot class.

    The snapshot provides a direct accessor to the ``Entity`` instances stored as deltas by the delta repository
    class.
    """

    async def get(
        self,
        name: Union[str, type[Entity], ModelType],
        uuid: UUID,
        transaction: Optional[TransactionEntry] = None,
        **kwargs,
    ) -> Entity:
        """Get a ``Entity`` instance from its identifier.

        :param name: Class name of the ``Entity``.
        :param uuid: Identifier of the ``Entity``.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param kwargs: Additional named arguments.
        :return: The ``Entity`` instance.
        """
        snapshot_entry = await self.get_entry(name, uuid, transaction=transaction, **kwargs)
        instance = snapshot_entry.build(**kwargs)
        return instance

    async def get_entry(self, name: Union[str, type[Entity], ModelType], uuid: UUID, **kwargs) -> SnapshotEntry:
        """Get a ``SnapshotEntry`` from its identifier.

        :param name: Class name of the ``Entity``.
        :param uuid: Identifier of the ``Entity``.
        :param kwargs: Additional named arguments.
        :return: The ``SnapshotEntry`` instance.
        """

        try:
            return await self.find_entries(
                name, _EqualCondition("uuid", uuid), **kwargs | {"exclude_deleted": False}
            ).__anext__()
        except StopAsyncIteration:
            raise NotFoundException(f"The instance could not be found: {uuid!s}")

    def get_all(
        self,
        name: Union[str, type[Entity], ModelType],
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        **kwargs,
    ) -> AsyncIterator[Entity]:
        """Get all ``Entity`` instances.

        :param name: Class name of the ``Entity``.
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
        :return: An asynchronous iterator that containing the ``Entity`` instances.
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

    async def find_one(
        self,
        name: Union[str, type[Entity]],
        condition: _Condition,
        transaction: Optional[TransactionEntry] = None,
        **kwargs,
    ) -> Entity:
        """Find a ``Entity`` instance based on a ``Condition``.

        :param name: Class name of the ``Entity``.
        :param condition: The condition that must be satisfied by the ``Entity`` instances.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``Entity`` instances.
        """
        try:
            return await self.find(name, condition=condition, limit=1, transaction=transaction, **kwargs).__anext__()
        except StopAsyncIteration:
            raise NotFoundException(f"There are not any instance matching the given condition: {condition}")

    async def find(
        self,
        name: Union[str, type[Entity], ModelType],
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        **kwargs,
    ) -> AsyncIterator[Entity]:
        """Find a collection of ``Entity`` instances based on a ``Condition``.

        :param name: Class name of the ``Entity``.
        :param condition: The condition that must be satisfied by the ``Entity`` instances.
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
        :return: An asynchronous iterator that containing the ``Entity`` instances.
        """
        iterable = self.find_entries(
            name=name,
            condition=condition,
            ordering=ordering,
            limit=limit,
            streaming_mode=streaming_mode,
            transaction=transaction,
            **kwargs,
        )
        async for snapshot_entry in iterable:
            yield snapshot_entry.build(**kwargs)

    async def find_entries(
        self,
        name: Union[str, type[Entity], ModelType],
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        exclude_deleted: bool = True,
        synchronize: bool = True,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        """Find a collection of ``SnapshotEntry`` instances based on a ``Condition``.

        :param name: Class name of the ``Entity``.
        :param condition: The condition that must be satisfied by the ``Entity`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param streaming_mode: If ``True`` return the values in streaming directly from the database (keep an open
            database connection), otherwise preloads the full set of values on memory and then retrieves them.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param exclude_deleted: If ``True``, deleted ``Entity`` entries are included, otherwise deleted
            ``Entity`` entries are filtered.
        :param synchronize: If ``True`` a synchronization is performed before processing the query, otherwise the query
            is performed without any synchronization step.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``Entity`` instances.
        """
        if isinstance(name, ModelType):
            name = name.model_cls
        if isinstance(name, type):
            name = classname(name)

        if transaction is None:
            transaction = TRANSACTION_CONTEXT_VAR.get()

        if synchronize:
            await self.synchronize(**kwargs)

        iterable = self._find_entries(
            name=name,
            condition=condition,
            ordering=ordering,
            limit=limit,
            streaming_mode=streaming_mode,
            transaction=transaction,
            exclude_deleted=exclude_deleted,
            **kwargs,
        )
        async for entry in iterable:
            yield entry

    @abstractmethod
    def _find_entries(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        streaming_mode: bool,
        transaction: Optional[TransactionEntry],
        exclude_deleted: bool,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        raise NotImplementedError

    def synchronize(self, **kwargs) -> Awaitable[None]:
        """Synchronize the snapshot to the latest available version.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return self._synchronize(**kwargs, synchronize=False)

    @abstractmethod
    async def _synchronize(self, **kwargs) -> None:
        raise NotImplementedError
