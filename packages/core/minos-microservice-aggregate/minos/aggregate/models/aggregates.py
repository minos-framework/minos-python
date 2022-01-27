from __future__ import (
    annotations,
)

import logging
from datetime import (
    datetime,
)
from typing import (
    AsyncIterator,
    Optional,
    Type,
    TypeVar,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    NULL_DATETIME,
    NULL_UUID,
    NotProvidedException,
)

from ..events import (
    EventEntry,
    EventRepository,
)
from ..exceptions import (
    EventRepositoryException,
)
from ..queries import (
    _Condition,
    _Ordering,
)
from ..snapshots import (
    SnapshotRepository,
)
from .diffs import (
    AggregateDiff,
    IncrementalFieldDiff,
)
from .entities import (
    Entity,
)

logger = logging.getLogger(__name__)


class Aggregate(Entity):
    """Base aggregate class."""

    version: int
    created_at: datetime
    updated_at: datetime

    @inject
    def __init__(
        self,
        *args,
        uuid: UUID = NULL_UUID,
        version: int = 0,
        created_at: datetime = NULL_DATETIME,
        updated_at: datetime = NULL_DATETIME,
        _repository: EventRepository = Provide["event_repository"],
        _snapshot: SnapshotRepository = Provide["snapshot_repository"],
        **kwargs,
    ):

        super().__init__(version, created_at, updated_at, *args, uuid=uuid, **kwargs)

        if _repository is None or isinstance(_repository, Provide):
            raise NotProvidedException("An event repository instance is required.")
        if _snapshot is None or isinstance(_snapshot, Provide):
            raise NotProvidedException("A snapshot instance is required.")

        self._repository = _repository
        self._snapshot = _snapshot

    @classmethod
    @inject
    async def get(
        cls: Type[T], uuid: UUID, _snapshot: SnapshotRepository = Provide["snapshot_repository"], **kwargs
    ) -> T:
        """Get one instance from the database based on its identifier.

        :param uuid: The identifier of the instance.
        :param _snapshot: Snapshot to be set to the aggregate.
        :return: A list of aggregate instances.
        """
        if _snapshot is None or isinstance(_snapshot, Provide):
            raise NotProvidedException("A snapshot instance is required.")

        # noinspection PyTypeChecker
        return await _snapshot.get(cls.classname, uuid, _snapshot=_snapshot, **kwargs)

    @classmethod
    @inject
    async def find(
        cls: Type[T],
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        _snapshot: SnapshotRepository = Provide["snapshot_repository"],
        **kwargs,
    ) -> AsyncIterator[T]:
        """Find a collection of instances based on a given ``Condition``.

        :param condition: The ``Condition`` that must be satisfied by all the instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param _snapshot: Snapshot to be set to the aggregate.
        :return: A list of aggregate instances.
        :return: An aggregate instance.
        """
        if _snapshot is None or isinstance(_snapshot, Provide):
            raise NotProvidedException("A snapshot instance is required.")
        # noinspection PyTypeChecker
        iterable = _snapshot.find(cls.classname, condition, ordering, limit, _snapshot=_snapshot, **kwargs)
        # noinspection PyTypeChecker
        async for aggregate in iterable:
            yield aggregate

    @classmethod
    async def create(cls: Type[T], *args, **kwargs,) -> T:
        """Create a new ``Aggregate`` instance.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``Aggregate`` instance.
        """
        if "uuid" in kwargs:
            raise EventRepositoryException(
                f"The identifier must be computed internally on the repository. Obtained: {kwargs['uuid']}"
            )
        if "version" in kwargs:
            raise EventRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )
        if "created_at" in kwargs:
            raise EventRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['created_at']}"
            )
        if "updated_at" in kwargs:
            raise EventRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['updated_at']}"
            )

        instance: T = cls(*args, **kwargs)

        aggregate_diff = AggregateDiff.from_aggregate(instance)
        entry = await instance._repository.submit(aggregate_diff)

        instance._update_from_repository_entry(entry)

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    async def update(self: T, **kwargs) -> T:
        """Update an existing ``Aggregate`` instance.

        :param kwargs: Additional named arguments.
        :return: An updated ``Aggregate``  instance.
        """

        if "version" in kwargs:
            raise EventRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )
        if "created_at" in kwargs:
            raise EventRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['created_at']}"
            )
        if "updated_at" in kwargs:
            raise EventRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['updated_at']}"
            )

        for key, value in kwargs.items():
            setattr(self, key, value)

        previous = await self.get(self.uuid, _repository=self._repository, _snapshot=self._snapshot)
        aggregate_diff = self.diff(previous)
        if not len(aggregate_diff.fields_diff):
            return self

        entry = await self._repository.submit(aggregate_diff)

        self._update_from_repository_entry(entry)

        return self

    async def save(self) -> None:
        """Store the current instance on the repository.

        If didn't exist previously creates a new one, otherwise updates the existing one.
        """
        is_creation = self.uuid == NULL_UUID
        if is_creation != (self.version == 0):
            if is_creation:
                raise EventRepositoryException(
                    f"The version must be computed internally on the repository. Obtained: {self.version}"
                )
            else:
                raise EventRepositoryException(
                    f"The uuid must be computed internally on the repository. Obtained: {self.uuid}"
                )

        values = {
            k: field.value
            for k, field in self.fields.items()
            if k not in {"uuid", "version", "created_at", "updated_at"}
        }
        if is_creation:
            new = await self.create(**values, _repository=self._repository, _snapshot=self._snapshot)
            self._fields |= new.fields
        else:
            await self.update(
                **values, _repository=self._repository, _snapshot=self._snapshot,
            )

    async def refresh(self) -> None:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await type(self).get(self.uuid, _repository=self._repository, _snapshot=self._snapshot)
        self._fields |= new.fields

    async def delete(self) -> None:
        """Delete the given aggregate instance.

        :return: This method does not return anything.
        """
        aggregate_diff = AggregateDiff.from_deleted_aggregate(self)
        entry = await self._repository.submit(aggregate_diff)

        self._update_from_repository_entry(entry)

    def _update_from_repository_entry(self, entry: EventEntry) -> None:
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

    def apply_diff(self, aggregate_diff: AggregateDiff) -> None:
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
