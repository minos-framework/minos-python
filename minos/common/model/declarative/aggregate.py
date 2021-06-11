"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    AsyncIterator,
    Generic,
    NoReturn,
    Optional,
    TypeVar,
)

from dependency_injector.wiring import (
    Provide,
)

from ...exceptions import (
    MinosBrokerNotProvidedException,
    MinosRepositoryManuallySetAggregateIdException,
    MinosRepositoryManuallySetAggregateVersionException,
    MinosRepositoryNotProvidedException,
    MinosSnapshotNotProvidedException,
)
from ...networks import (
    MinosBroker,
)
from ...repository import (
    MinosRepository,
)
from ...snapshot import (
    MinosSnapshot,
)
from .abc import (
    DeclarativeModel,
)

T = TypeVar("T")


class Aggregate(DeclarativeModel, Generic[T]):
    """Base aggregate class."""

    id: int
    version: int

    _broker: MinosBroker = Provide["event_broker"]
    _repository: MinosRepository = Provide["repository"]
    _snapshot: MinosSnapshot = Provide["snapshot"]

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        id: int,
        version: int,
        *args,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        _snapshot: Optional[MinosSnapshot] = None,
        **kwargs,
    ):

        super().__init__(id, version, *args, **kwargs)

        if _broker is not None:
            self._broker = _broker
        if _repository is not None:
            self._repository = _repository
        if _snapshot is not None:
            self._snapshot = _snapshot

        if self._broker is None or isinstance(self._broker, Provide):
            raise MinosBrokerNotProvidedException("A broker instance is required.")
        if self._repository is None or isinstance(self._repository, Provide):
            raise MinosRepositoryNotProvidedException("A repository instance is required.")
        if self._snapshot is None or isinstance(self._snapshot, Provide):
            raise MinosSnapshotNotProvidedException("A snapshot instance is required.")

    @classmethod
    async def get(
        cls,
        ids: list[int],
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        _snapshot: Optional[MinosSnapshot] = None,
    ) -> AsyncIterator[T]:
        """Get a sequence of aggregates based on a list of identifiers.

        :param ids: list of identifiers.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :param _snapshot: Snapshot to be set to the aggregate.
        :return: A list of aggregate instances.
        """

        if _broker is None:
            _broker = cls._broker
            if isinstance(_broker, Provide):
                raise MinosBrokerNotProvidedException("A broker instance is required.")

        if _repository is None:
            _repository = cls._repository
            if isinstance(_repository, Provide):
                raise MinosRepositoryNotProvidedException("A repository instance is required.")

        if _snapshot is None:
            _snapshot = cls._snapshot
            if isinstance(_snapshot, Provide):
                raise MinosSnapshotNotProvidedException("A snapshot instance is required.")

        # noinspection PyTypeChecker
        iterable = _snapshot.get(cls.classname, ids, _broker=_broker, _repository=_repository, _snapshot=_snapshot)

        # noinspection PyTypeChecker
        async for aggregate in iterable:
            yield aggregate

    # noinspection PyShadowingBuiltins
    @classmethod
    async def get_one(
        cls,
        id: int,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        _snapshot: Optional[MinosSnapshot] = None,
    ) -> T:
        """Get one aggregate based on an identifier.

        :param id: aggregate identifier.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :param _snapshot: Snapshot to be set to the aggregate.
        :return: A list of aggregate instances.
        :return: An aggregate instance.
        """
        return await cls.get([id], _broker=_broker, _repository=_repository, _snapshot=_snapshot).__anext__()

    @classmethod
    async def create(
        cls, *args, _broker: Optional[MinosBroker] = None, _repository: Optional[MinosRepository] = None, **kwargs
    ) -> T:
        """Create a new ``Aggregate`` instance.

        :param args: Additional positional arguments.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :param kwargs: Additional named arguments.
        :return: A new ``Aggregate`` instance.
        """
        if "id" in kwargs:
            raise MinosRepositoryManuallySetAggregateIdException(
                f"The id must be computed internally on the repository. Obtained: {kwargs['id']}"
            )

        if "version" in kwargs:
            raise MinosRepositoryManuallySetAggregateVersionException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )

        if _broker is None:
            _broker = cls._broker

        instance = cls(0, 0, *args, _broker=_broker, _repository=_repository, **kwargs)

        entry = await instance._repository.insert(instance)

        instance.id = entry.aggregate_id
        instance.version = entry.version

        await instance._broker.send_one(instance, topic=f"{type(instance).__name__}Created")

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    async def update(self, **kwargs) -> T:
        """Update an existing ``Aggregate`` instance.

        :param kwargs: Additional named arguments.
        :return: An updated ``Aggregate``  instance.
        """
        if "version" in kwargs:
            raise MinosRepositoryManuallySetAggregateVersionException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )

        # Update model...
        for key, value in kwargs.items():
            setattr(self, key, value)

        entry = await self._repository.update(self)

        self.id = entry.aggregate_id
        self.version = entry.version

        await self._broker.send_one(self, topic=f"{type(self).__name__}Updated")

        return self

    async def refresh(self) -> NoReturn:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await type(self).get_one(
            self.id, _broker=self._broker, _repository=self._repository, _snapshot=self._snapshot
        )
        self._fields |= new.fields

    async def delete(self) -> NoReturn:
        """Delete the given aggregate instance.

        :return: This method does not return anything.
        """
        await self._repository.delete(self)
        await self._broker.send_one(self, topic=f"{type(self).__name__}Deleted")
