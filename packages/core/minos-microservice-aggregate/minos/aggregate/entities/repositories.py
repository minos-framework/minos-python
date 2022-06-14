import logging
from collections.abc import (
    AsyncIterator,
)
from typing import (
    Optional,
    TypeVar,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    Inject,
    NotProvidedException,
)

from ..actions import (
    Action,
)
from ..deltas import (
    Delta,
    DeltaEntry,
    DeltaRepository,
)
from ..exceptions import (
    DeltaRepositoryException,
)
from ..queries import (
    _Condition,
    _Ordering,
)
from ..snapshots import (
    SnapshotRepository,
)
from .models import (
    Entity,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Entity)


class EntityRepository:
    """Entity Repository class."""

    _delta_repository: DeltaRepository
    _snapshot_repository: SnapshotRepository

    # noinspection PyUnusedLocal
    @Inject()
    def __init__(
        self,
        delta_repository: DeltaRepository,
        snapshot_repository: SnapshotRepository,
        *args,
        **kwargs,
    ):

        if delta_repository is None:
            raise NotProvidedException(f"A {DeltaRepository!r} instance is required.")
        if snapshot_repository is None:
            raise NotProvidedException(f"A {SnapshotRepository!r} instance is required.")

        self._delta_repository = delta_repository
        self._snapshot_repository = snapshot_repository

    async def get(self, type_: type[T], uuid: UUID, **kwargs) -> T:
        """Get one instance from the database based on its identifier.

        :param type_: The of the entity to be looked for.
        :param uuid: The identifier of the instance.
        :return: A ``Entity`` instance.
        """
        # noinspection PyTypeChecker
        return await self._snapshot_repository.get(type_, uuid, **kwargs)

    def get_all(
        self,
        type_: type[T],
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[T]:
        """Get all instance from the database.

        :param type_: The of the entity to be looked for.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :return: A ``Entity`` instance.
        """
        # noinspection PyTypeChecker
        return self._snapshot_repository.get_all(type_, ordering, limit, **kwargs)

    async def find_one(self, type_: type[T], condition: _Condition, **kwargs) -> T:
        """Find a ``Entity`` instance based on a ``Condition``.

        :param type_: The type of the entity to be looked for.
        :param condition: The condition that must be satisfied by the ``Entity`` instances.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``Entity`` instances.
        """
        return await self._snapshot_repository.find_one(type_, condition, **kwargs)

    def find(
        self,
        type_: type[T],
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[T]:
        """Find a collection of instances based on a given ``Condition``.

        :param type_: The type of the entity to be looked for.
        :param condition: The ``Condition`` that must be satisfied by all the instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :return: An asynchronous iterator of ``Entity`` instances.
        """
        return self._snapshot_repository.find(type_, condition, ordering, limit, **kwargs)

    async def create(self, type_or_instance: Union[T, type[T]], *args, **kwargs) -> tuple[T, Delta]:
        """Create a new ``Entity`` instance.

        :param type_or_instance: The instance to be created. If it is a ``type`` then the instance is created internally
            using ``args`` and ``kwargs`` as parameters.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``Entity`` instance.
        """
        if "uuid" in kwargs:
            raise DeltaRepositoryException(
                f"The identifier must be computed internally on the repository. Obtained: {kwargs['uuid']}"
            )
        if "version" in kwargs:
            raise DeltaRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )
        if "created_at" in kwargs:
            raise DeltaRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['created_at']}"
            )
        if "updated_at" in kwargs:
            raise DeltaRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['updated_at']}"
            )
        if isinstance(type_or_instance, type):
            instance: T = type_or_instance(*args, **kwargs)
        else:
            instance = type_or_instance
            if len(args) or len(kwargs):
                raise DeltaRepositoryException(
                    f"Additional parameters are not provided when passing an already built {Entity!r} instance. "
                    f"Obtained: args={args!r}, kwargs={kwargs!r}"
                )

        delta = Delta.from_entity(instance)
        entry = await self._delta_repository.submit(delta)

        self._update_from_repository_entry(instance, entry)

        return instance, entry.delta

    # noinspection PyMethodParameters,PyShadowingBuiltins
    async def update(self, instance: T, **kwargs) -> tuple[T, Optional[Delta]]:
        """Update an existing ``Entity`` instance.

        :param instance: The instance to be updated.
        :param kwargs: Additional named arguments.
        :return: An updated ``Entity``  instance.
        """

        if "version" in kwargs:
            raise DeltaRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )
        if "created_at" in kwargs:
            raise DeltaRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['created_at']}"
            )
        if "updated_at" in kwargs:
            raise DeltaRepositoryException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['updated_at']}"
            )

        for key, value in kwargs.items():
            setattr(instance, key, value)

        previous = await self.get(type(instance), instance.uuid)
        delta = instance.diff(previous)
        if not len(delta.fields_diff):
            return instance, None

        entry = await self._delta_repository.submit(delta)

        self._update_from_repository_entry(instance, entry)

        return instance, entry.delta

    async def save(self, instance: T) -> Optional[Delta]:
        """Store the current instance on the repository.

        If didn't exist previously creates a new one, otherwise updates the existing one.
        """
        is_creation = instance.uuid == NULL_UUID
        if is_creation != (instance.version == 0):
            if is_creation:
                raise DeltaRepositoryException(
                    f"The version must be computed internally on the repository. Obtained: {instance.version}"
                )
            else:
                raise DeltaRepositoryException(
                    f"The uuid must be computed internally on the repository. Obtained: {instance.uuid}"
                )

        values = {
            k: field.value
            for k, field in instance.fields.items()
            if k not in {"uuid", "version", "created_at", "updated_at"}
        }
        if is_creation:
            new, delta = await self.create(type(instance), **values)
            instance._fields |= new.fields
        else:
            _, delta = await self.update(instance, **values)
        return delta

    async def refresh(self, instance: T) -> None:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await self.get(type(instance), instance.uuid)
        instance._fields |= new.fields

    async def delete(self, instance: T) -> Delta:
        """Delete the given root entity instance.

        :return: This method does not return anything.
        """
        delta = Delta.from_entity(instance, action=Action.DELETE)
        entry = await self._delta_repository.submit(delta)

        self._update_from_repository_entry(instance, entry)
        return entry.delta

    @staticmethod
    def _update_from_repository_entry(instance: T, entry: DeltaEntry) -> None:
        instance.uuid = entry.uuid
        instance.version = entry.version
        if entry.action.is_create:
            instance.created_at = entry.created_at
        instance.updated_at = entry.created_at
