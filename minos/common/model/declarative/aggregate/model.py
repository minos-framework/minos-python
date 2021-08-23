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
from datetime import (
    datetime,
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

from ....datetime import (
    NULL_DATETIME,
)
from ....exceptions import (
    MinosBrokerNotProvidedException,
    MinosRepositoryException,
    MinosRepositoryNotProvidedException,
    MinosSnapshotNotProvidedException,
)
from ....networks import (
    MinosBroker,
)
from ....repository import (
    MinosRepository,
    RepositoryEntry,
)
from ....snapshot import (
    MinosSnapshot,
)
from ....uuid import (
    NULL_UUID,
)
from ...dynamic import (
    IncrementalFieldDiff,
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
    created_at: datetime
    updated_at: datetime

    _broker: MinosBroker = Provide["event_broker"]
    _repository: MinosRepository = Provide["repository"]
    _snapshot: MinosSnapshot = Provide["snapshot"]

    def __init__(
        self,
        *args,
        uuid: UUID = NULL_UUID,
        version: int = 0,
        created_at: datetime = NULL_DATETIME,
        updated_at: datetime = NULL_DATETIME,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        _snapshot: Optional[MinosSnapshot] = None,
        **kwargs,
    ):

        super().__init__(version, created_at, updated_at, *args, uuid=uuid, **kwargs)

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
            raise MinosRepositoryException(
                f"The identifier must be computed internally on the repository. Obtained: {kwargs['uuid']}"
            )

        if "version" in kwargs:
            raise MinosRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )
        if "created_at" in kwargs:
            raise MinosRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['created_at']}"
            )
        if "updated_at" in kwargs:
            raise MinosRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['updated_at']}"
            )

        instance: T = cls(*args, _broker=_broker, _repository=_repository, **kwargs)

        aggregate_diff = AggregateDiff.from_aggregate(instance)
        entry = await instance._repository.create(aggregate_diff)

        instance._update_from_repository_entry(entry)
        aggregate_diff.update_from_repository_entry(entry)

        await instance._broker.send(aggregate_diff, topic=f"{type(instance).__name__}Created")

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    async def update(self: T, **kwargs) -> T:
        """Update an existing ``Aggregate`` instance.

        :param kwargs: Additional named arguments.
        :return: An updated ``Aggregate``  instance.
        """

        if "version" in kwargs:
            raise MinosRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )
        if "created_at" in kwargs:
            raise MinosRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['created_at']}"
            )
        if "updated_at" in kwargs:
            raise MinosRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['updated_at']}"
            )

        for key, value in kwargs.items():
            setattr(self, key, value)

        previous = await self.get_one(
            self.uuid, _broker=self._broker, _repository=self._repository, _snapshot=self._snapshot
        )
        aggregate_diff = self.diff(previous)
        if not len(aggregate_diff.fields_diff):
            return self

        entry = await self._repository.update(aggregate_diff)

        self._update_from_repository_entry(entry)
        aggregate_diff.update_from_repository_entry(entry)

        await self._send_update_events(aggregate_diff)

        return self

    async def _send_update_events(self, aggregate_diff: AggregateDiff):
        futures = [self._broker.send(aggregate_diff, topic=f"{type(self).__name__}Updated")]

        for decomposed_aggregate_diff in aggregate_diff.decompose():
            diff = next(iter(decomposed_aggregate_diff.fields_diff.flatten_values()))
            topic = f"{type(self).__name__}Updated.{diff.name}"
            if isinstance(diff, IncrementalFieldDiff):
                topic += f".{diff.action.value}"
            futures.append(self._broker.send(decomposed_aggregate_diff, topic=topic))

        await gather(*futures)

    async def save(self) -> NoReturn:
        """Store the current instance on the repository.

        If didn't exist previously creates a new one, otherwise updates the existing one.
        """
        is_creation = self.uuid == NULL_UUID
        if is_creation != (self.version == 0):
            if is_creation:
                raise MinosRepositoryException(
                    f"The version must be computed internally on the repository. Obtained: {self.version}"
                )
            else:
                raise MinosRepositoryException(
                    f"The uuid must be computed internally on the repository. Obtained: {self.uuid}"
                )

        values = {
            k: field.value
            for k, field in self.fields.items()
            if k not in {"uuid", "version", "created_at", "updated_at"}
        }
        if is_creation:
            new = await self.create(
                **values, _broker=self._broker, _repository=self._repository, _snapshot=self._snapshot
            )
            self._fields |= new.fields
        else:
            await self.update(
                **values, _broker=self._broker, _repository=self._repository, _snapshot=self._snapshot,
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
        aggregate_diff = AggregateDiff.from_deleted_aggregate(self)
        entry = await self._repository.delete(aggregate_diff)

        self._update_from_repository_entry(entry)
        aggregate_diff.update_from_repository_entry(entry)

        await self._broker.send(aggregate_diff, topic=f"{type(self).__name__}Deleted")

    def _update_from_repository_entry(self, entry: RepositoryEntry) -> None:
        self.uuid = entry.aggregate_uuid
        self.version = entry.version
        if entry.action.is_create:
            self.created_at = entry.created_at
        self.updated_at = entry.created_at

    def diff(self, another: Aggregate) -> AggregateDiff:
        """Compute the difference with another aggregate.

        Both ``Aggregate`` instances (``self`` and ``another``) must share the same ``uuid`` value.

        :param another: Another ``Aggregate`` instance.
        :return: An ``FieldDiffContainer`` instance.
        """
        return AggregateDiff.from_difference(self, another)

    def apply_diff(self, aggregate_diff: AggregateDiff) -> NoReturn:
        """Apply the differences over the instance.

        :param aggregate_diff: The ``FieldDiffContainer`` containing the values to be set.
        :return: This method does not return anything.
        """
        if self.uuid != aggregate_diff.uuid:
            raise ValueError(
                f"To apply the difference, it must have same uuid. "
                f"Expected: {self.uuid!r} Obtained: {aggregate_diff.uuid!r}"
            )

        logger.debug(f"Applying {aggregate_diff!r} to {self!r}...")
        for diff in aggregate_diff.fields_diff.flatten_values():
            if isinstance(diff, IncrementalFieldDiff):
                container = getattr(self, diff.name)
                if diff.action.is_delete:
                    container.discard(diff.value)
                else:
                    container.add(diff.value)
            else:
                setattr(self, diff.name, diff.value)
        self.version = aggregate_diff.version
        self.updated_at = aggregate_diff.created_at

    @classmethod
    def from_diff(cls: Type[T], aggregate_diff: AggregateDiff, *args, **kwargs) -> T:
        """Build a new instance from an ``AggregateDiff``.

        :param aggregate_diff: The difference that contains the data.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``Aggregate`` instance.
        """
        return cls(
            *args,
            uuid=aggregate_diff.uuid,
            version=aggregate_diff.version,
            created_at=aggregate_diff.created_at,
            updated_at=aggregate_diff.created_at,
            **aggregate_diff.get_all(),
            **kwargs,
        )


T = TypeVar("T", bound=Aggregate)
