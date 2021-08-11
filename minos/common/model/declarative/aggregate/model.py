"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from asyncio import (
    gather,
)
from typing import (
    AsyncIterator,
    NoReturn,
    Optional,
    Type,
    TypeVar,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
)

from ....constants import (
    NULL_UUID,
)
from ....exceptions import (
    MinosBrokerNotProvidedException,
    MinosRepositoryManuallySetAggregateIdentifierException,
    MinosRepositoryManuallySetAggregateVersionException,
    MinosRepositoryNotProvidedException,
    MinosSnapshotNotProvidedException,
)
from ....networks import (
    MinosBroker,
)
from ....repository import (
    MinosRepository,
)
from ....snapshot import (
    MinosSnapshot,
)
from ..entities import (
    Entity,
)
from .diff import (
    AggregateDiff,
)

logger = logging.getLogger(__name__)


class Aggregate(Entity):
    """Base aggregate class."""

    version: int

    _broker: MinosBroker = Provide["event_broker"]
    _repository: MinosRepository = Provide["repository"]
    _snapshot: MinosSnapshot = Provide["snapshot"]

    def __init__(
        self,
        *args,
        uuid: UUID = NULL_UUID,
        version: int = 0,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        _snapshot: Optional[MinosSnapshot] = None,
        **kwargs,
    ):

        super().__init__(version, *args, uuid=uuid, **kwargs)

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
        cls: Type[T],
        uuids: set[UUID],
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        _snapshot: Optional[MinosSnapshot] = None,
    ) -> AsyncIterator[T]:
        """Get a sequence of aggregates based on a list of identifiers.

        :param uuids: set of identifiers.
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
        iterable = _snapshot.get(cls.classname, uuids, _broker=_broker, _repository=_repository, _snapshot=_snapshot)

        # noinspection PyTypeChecker
        async for aggregate in iterable:
            yield aggregate

    @classmethod
    async def get_one(
        cls: Type[T],
        uuid: UUID,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        _snapshot: Optional[MinosSnapshot] = None,
    ) -> T:
        """Get one aggregate based on an identifier.

        :param uuid: Identifier of the aggregate.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :param _snapshot: Snapshot to be set to the aggregate.
        :return: A list of aggregate instances.
        :return: An aggregate instance.
        """
        return await cls.get({uuid}, _broker=_broker, _repository=_repository, _snapshot=_snapshot).__anext__()

    @classmethod
    async def create(
        cls: Type[T],
        *args,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        **kwargs,
    ) -> T:
        """Create a new ``Aggregate`` instance.

        :param args: Additional positional arguments.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :param kwargs: Additional named arguments.
        :return: A new ``Aggregate`` instance.
        """
        if "uuid" in kwargs:
            raise MinosRepositoryManuallySetAggregateIdentifierException(
                f"The identifier must be computed internally on the repository. Obtained: {kwargs['uuid']}"
            )

        if "version" in kwargs:
            raise MinosRepositoryManuallySetAggregateVersionException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )

        instance = cls(*args, _broker=_broker, _repository=_repository, **kwargs)

        diff = AggregateDiff.from_aggregate(instance)
        entry = await instance._repository.create(diff)

        instance.uuid, instance.version = entry.aggregate_uuid, entry.version
        diff.uuid, diff.version = entry.aggregate_uuid, entry.version

        await instance._broker.send(diff, topic=f"{type(instance).__name__}Created")

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    async def update(self: T, **kwargs) -> T:
        """Update an existing ``Aggregate`` instance.

        :param kwargs: Additional named arguments.
        :return: An updated ``Aggregate``  instance.
        """

        if "version" in kwargs:
            raise MinosRepositoryManuallySetAggregateVersionException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )

        for key, value in kwargs.items():
            setattr(self, key, value)

        previous = await self.get_one(
            self.uuid, _broker=self._broker, _repository=self._repository, _snapshot=self._snapshot
        )
        diff = AggregateDiff.from_difference(self, previous)
        entry = await self._repository.update(diff)

        self.uuid, self.version = entry.aggregate_uuid, entry.version
        diff.uuid, diff.version = entry.aggregate_uuid, entry.version

        await self._send_update_events(diff)

        return self

    async def _send_update_events(self, diff: AggregateDiff):

        futures = [self._broker.send(diff, topic=f"{type(self).__name__}Updated")]

        for aggr in diff.decompose():
            topic = f"{type(self).__name__}Updated.{list(aggr.differences.fields.keys())[0]}"
            futures.append(self._broker.send(aggr, topic=topic))

        await gather(*futures)

    async def save(self) -> NoReturn:
        """Store the current instance on the repository.

        If didn't exist previously creates a new one, otherwise updates the existing one.
        """
        is_creation = self.uuid == NULL_UUID
        if is_creation != (self.version == 0):
            if is_creation:
                raise MinosRepositoryManuallySetAggregateVersionException(
                    f"The version must be computed internally on the repository. Obtained: {self.version}"
                )
            else:
                raise MinosRepositoryManuallySetAggregateIdentifierException(
                    f"The uuid must be computed internally on the repository. Obtained: {self.uuid}"
                )

        if is_creation:
            new = await self.create(
                **{k: field.value for k, field in self.fields.items() if k not in ("uuid", "version")},
                _broker=self._broker,
                _repository=self._repository,
                _snapshot=self._snapshot,
            )
            self._fields |= new.fields
        else:
            await self.update(
                **{k: field.value for k, field in self.fields.items() if k not in ("uuid", "version")},
                _broker=self._broker,
                _repository=self._repository,
                _snapshot=self._snapshot,
            )

    async def refresh(self) -> NoReturn:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await type(self).get_one(
            self.uuid, _broker=self._broker, _repository=self._repository, _snapshot=self._snapshot
        )
        self._fields |= new.fields

    async def delete(self) -> NoReturn:
        """Delete the given aggregate instance.

        :return: This method does not return anything.
        """
        diff = AggregateDiff.from_deleted_aggregate(self)
        entry = await self._repository.delete(diff)

        self.uuid, self.version = entry.aggregate_uuid, entry.version
        diff.uuid, diff.version = entry.aggregate_uuid, entry.version

        await self._broker.send(diff, topic=f"{type(self).__name__}Deleted")

    def diff(self, another: Aggregate) -> AggregateDiff:
        """Compute the difference with another aggregate.

        Both ``Aggregate`` instances (``self`` and ``another``) must share the same ``uuid`` value.

        :param another: Another ``Aggregate`` instance.
        :return: An ``FieldsDiff`` instance.
        """
        return AggregateDiff.from_difference(self, another)

    def apply_diff(self, diff: AggregateDiff) -> NoReturn:
        """Apply the differences over the instance.

        :param diff: The ``FieldsDiff`` containing the values to be set.
        :return: This method does not return anything.
        """
        if self.uuid != diff.uuid:
            raise ValueError(
                f"To apply the difference, it must have same uuid. " f"Expected: {self.uuid!r} Obtained: {diff.uuid!r}"
            )
        logger.debug(f"Applying {diff!r} to {self!r}...")
        for field in diff.differences:
            setattr(self, field.name, field.value)
        self.version = diff.version

    @classmethod
    def from_diff(cls: Type[T], diff: AggregateDiff, *args, **kwargs) -> T:
        """Build a new instance from an ``AggregateDiff``.

        :param diff: The difference that contains the data.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``Aggregate`` instance.
        """
        # noinspection PyArgumentList
        return cls(*args, uuid=diff.uuid, version=diff.version, **diff.differences, **kwargs)


T = TypeVar("T", bound=Aggregate)
