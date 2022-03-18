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
    uuid4,
)

from minos.common import (
    NULL_DATETIME,
    NULL_UUID,
    DeclarativeModel,
    Inject,
    NotProvidedException,
)

from ..events import (
    Event,
    EventEntry,
    EventRepository,
    IncrementalFieldDiff,
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

logger = logging.getLogger(__name__)


class Entity(DeclarativeModel):
    """Entity class."""

    uuid: UUID

    def __init__(self, *args, uuid: Optional[UUID] = None, **kwargs):
        if uuid is None:
            uuid = uuid4()
        super().__init__(uuid, *args, **kwargs)


class ExternalEntity(Entity):
    """External Entity class."""

    version: int

    def __init__(self, uuid: UUID, *args, **kwargs):
        super().__init__(uuid=uuid, *args, **kwargs)


T = TypeVar("T", bound="RootEntity")


class RootEntity(Entity):
    """Base Root Entity class."""

    version: int
    created_at: datetime
    updated_at: datetime

    _event_repository: EventRepository
    _snapshot_repository: SnapshotRepository

    @Inject()
    def __init__(
        self,
        *args,
        uuid: UUID = NULL_UUID,
        version: int = 0,
        created_at: datetime = NULL_DATETIME,
        updated_at: datetime = NULL_DATETIME,
        _event_repository: EventRepository,
        _snapshot_repository: SnapshotRepository,
        **kwargs,
    ):

        super().__init__(version, created_at, updated_at, *args, uuid=uuid, **kwargs)

        if _event_repository is None:
            raise NotProvidedException(f"A {EventRepository!r} instance is required.")
        if _snapshot_repository is None:
            raise NotProvidedException(f"A {SnapshotRepository!r} instance is required.")

        self._event_repository = _event_repository
        self._snapshot_repository = _snapshot_repository

    @classmethod
    @Inject()
    async def get(cls: Type[T], uuid: UUID, *, _snapshot_repository: SnapshotRepository, **kwargs) -> T:
        """Get one instance from the database based on its identifier.

        :param uuid: The identifier of the instance.
        :param _snapshot_repository: Snapshot to be set to the root entity.
        :return: A ``RootEntity`` instance.
        """
        if _snapshot_repository is None:
            raise NotProvidedException(f"A {SnapshotRepository!r} instance is required.")

        # noinspection PyTypeChecker
        return await _snapshot_repository.get(cls.classname, uuid, _snapshot_repository=_snapshot_repository, **kwargs)

    @classmethod
    @Inject()
    def get_all(
        cls: Type[T],
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        *,
        _snapshot_repository: SnapshotRepository,
        **kwargs,
    ) -> AsyncIterator[T]:
        """Get all instance from the database.

        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param _snapshot_repository: Snapshot to be set to the root entity.
        :return: A ``RootEntity`` instance.
        """
        if _snapshot_repository is None:
            raise NotProvidedException(f"A {SnapshotRepository!r} instance is required.")

        # noinspection PyTypeChecker
        return _snapshot_repository.get_all(
            cls.classname, ordering, limit, _snapshot_repository=_snapshot_repository, **kwargs
        )

    @classmethod
    @Inject()
    def find(
        cls: Type[T],
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        *,
        _snapshot_repository: SnapshotRepository,
        **kwargs,
    ) -> AsyncIterator[T]:
        """Find a collection of instances based on a given ``Condition``.

        :param condition: The ``Condition`` that must be satisfied by all the instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param _snapshot_repository: Snapshot to be set to the instances.
        :return: An asynchronous iterator of ``RootEntity`` instances.
        """
        if _snapshot_repository is None:
            raise NotProvidedException(f"A {SnapshotRepository!r} instance is required.")
        # noinspection PyTypeChecker
        return _snapshot_repository.find(
            cls.classname, condition, ordering, limit, _snapshot_repository=_snapshot_repository, **kwargs
        )

    @classmethod
    async def create(cls: Type[T], *args, **kwargs) -> T:
        """Create a new ``RootEntity`` instance.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``RootEntity`` instance.
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

        event = Event.from_root_entity(instance)
        entry = await instance._event_repository.submit(event)

        instance._update_from_repository_entry(entry)

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    async def update(self: T, **kwargs) -> T:
        """Update an existing ``RootEntity`` instance.

        :param kwargs: Additional named arguments.
        :return: An updated ``RootEntity``  instance.
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

        previous = await self.get(
            self.uuid, _event_repository=self._event_repository, _snapshot_repository=self._snapshot_repository
        )
        event = self.diff(previous)
        if not len(event.fields_diff):
            return self

        entry = await self._event_repository.submit(event)

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
            new = await self.create(
                **values, _event_repository=self._event_repository, _snapshot_repository=self._snapshot_repository
            )
            self._fields |= new.fields
        else:
            await self.update(
                **values, _event_repository=self._event_repository, _snapshot_repository=self._snapshot_repository
            )

    async def refresh(self) -> None:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await self.get(
            self.uuid, _event_repository=self._event_repository, _snapshot_repository=self._snapshot_repository
        )
        self._fields |= new.fields

    async def delete(self) -> None:
        """Delete the given root entity instance.

        :return: This method does not return anything.
        """
        event = Event.from_deleted_root_entity(self)
        entry = await self._event_repository.submit(event)

        self._update_from_repository_entry(entry)

    def _update_from_repository_entry(self, entry: EventEntry) -> None:
        self.uuid = entry.uuid
        self.version = entry.version
        if entry.action.is_create:
            self.created_at = entry.created_at
        self.updated_at = entry.created_at

    def diff(self, another: RootEntity) -> Event:
        """Compute the difference with another instance.

        Both ``RootEntity`` instances (``self`` and ``another``) must share the same ``uuid`` value.

        :param another: Another ``RootEntity`` instance.
        :return: An ``FieldDiffContainer`` instance.
        """
        return Event.from_difference(self, another)

    def apply_diff(self, event: Event) -> None:
        """Apply the differences over the instance.

        :param event: The ``FieldDiffContainer`` containing the values to be set.
        :return: This method does not return anything.
        """
        if self.uuid != event.uuid:
            raise ValueError(
                f"To apply the difference, it must have same uuid. " f"Expected: {self.uuid!r} Obtained: {event.uuid!r}"
            )

        logger.debug(f"Applying {event!r} to {self!r}...")
        for diff in event.fields_diff.flatten_values():
            if isinstance(diff, IncrementalFieldDiff):
                container = getattr(self, diff.name)
                if diff.action.is_delete:
                    container.discard(diff.value)
                else:
                    container.add(diff.value)
            else:
                setattr(self, diff.name, diff.value)
        self.version = event.version
        self.updated_at = event.created_at

    @classmethod
    def from_diff(cls: Type[T], event: Event, *args, **kwargs) -> T:
        """Build a new instance from an ``Event``.

        :param event: The difference that contains the data.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``RootEntity`` instance.
        """
        return cls(
            *args,
            uuid=event.uuid,
            version=event.version,
            created_at=event.created_at,
            updated_at=event.created_at,
            **event.get_fields(),
            **kwargs,
        )
