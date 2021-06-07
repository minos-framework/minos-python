"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from asyncio import (
    gather,
)
from operator import (
    attrgetter,
)
from typing import (
    Generic,
    NoReturn,
    Optional,
    TypeVar,
)

from dependency_injector.wiring import (
    Provide,
)

from ..exceptions import (
    MinosRepositoryAggregateNotFoundException,
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryManuallySetAggregateIdException,
    MinosRepositoryManuallySetAggregateVersionException,
    MinosRepositoryNonProvidedException,
)
from ..networks import (
    MinosBroker,
)
from ..repository import (
    MinosRepository,
    MinosRepositoryAction,
)
from .abc import (
    MinosModel,
)

T = TypeVar("T")


class Aggregate(MinosModel, Generic[T]):
    """Base aggregate class."""

    id: int
    version: int

    _broker: MinosBroker = Provide["event_broker"]
    _repository: MinosRepository = Provide["repository"]

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        id: int,
        version: int,
        *args,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        **kwargs,
    ):

        super().__init__(id, version, *args, **kwargs)

        if _broker is not None:
            self._broker = _broker
        if _repository is not None:
            self._repository = _repository

    @classmethod
    async def get(
        cls, ids: list[int], _broker: Optional[MinosBroker] = None, _repository: Optional[MinosRepository] = None,
    ) -> list[T]:
        """Get a sequence of aggregates based on a list of identifiers.

        :param ids: list of identifiers.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :return: A list of aggregate instances.
        """
        # noinspection PyShadowingBuiltins
        return list(await gather(*(cls.get_one(id, _broker, _repository) for id in ids)))

    # noinspection PyShadowingBuiltins
    @classmethod
    async def get_one(
        cls, id: int, _broker: Optional[MinosBroker] = None, _repository: Optional[MinosRepository] = None,
    ) -> T:
        """Get one aggregate based on an identifier.

        :param id: aggregate identifier.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :return: A list of aggregate instances.
        :return: An aggregate instance.
        """
        if _repository is None:
            _repository = cls._repository
        if _repository is None or isinstance(_repository, Provide):
            raise MinosRepositoryNonProvidedException("A repository instance is required.")

        # noinspection PyTypeChecker
        entries = [v async for v in _repository.select(aggregate_name=cls.classname, aggregate_id=id)]
        if not len(entries):
            raise MinosRepositoryAggregateNotFoundException(f"Not found any entries for the {repr(id)} id.")

        entry = max(entries, key=attrgetter("version"))
        if entry.action == MinosRepositoryAction.DELETE:
            raise MinosRepositoryDeletedAggregateException(f"The {id} id points to an already deleted aggregate.")

        instance = cls.from_avro_bytes(
            entry.data, id=entry.aggregate_id, version=entry.version, _broker=_broker, _repository=_repository
        )
        return instance

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

        if _repository is None:
            _repository = cls._repository
            if isinstance(_repository, Provide):
                raise MinosRepositoryNonProvidedException("A repository instance is required.")

        instance = cls(0, 0, *args, _broker=_broker, _repository=_repository, **kwargs)

        entry = await instance._repository.insert(instance)

        instance.id = entry.aggregate_id
        instance.version = entry.version

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

        if self._repository is None or isinstance(self._repository, Provide):
            raise MinosRepositoryNonProvidedException("A repository instance is required.")

        # Update model...
        for key, value in kwargs.items():
            setattr(self, key, value)

        entry = await self._repository.update(self)

        self.id = entry.aggregate_id
        self.version = entry.version

        return self

    async def refresh(self) -> NoReturn:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await type(self).get_one(self.id, _broker=self._broker, _repository=self._repository)
        self._fields |= new.fields

    async def delete(self) -> NoReturn:
        """Delete the given aggregate instance.

        :return: This method does not return anything.
        """
        if self._repository is None or isinstance(self._repository, Provide):
            raise MinosRepositoryNonProvidedException("A repository instance is required.")
        await self._repository.delete(self)
