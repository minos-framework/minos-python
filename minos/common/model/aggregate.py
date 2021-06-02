"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
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

from ..configuration import (
    MinosConfig,
)
from ..exceptions import (
    MinosRepositoryAggregateNotFoundException,
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryManuallySetAggregateIdException,
    MinosRepositoryManuallySetAggregateVersionException,
    MinosRepositoryNonProvidedException,
)
from ..meta import (
    self_or_classmethod,
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
        cls,
        ids: list[int],
        _config: Optional[MinosConfig] = None,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
    ) -> list[T]:
        """Get a sequence of aggregates based on a list of identifiers.

        :param ids: list of identifiers.
        :param _config: Current minos config.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :return: A list of aggregate instances.
        """
        # noinspection PyShadowingBuiltins
        return [await cls.get_one(id, _broker, _config, _repository) for id in ids]

    # noinspection PyShadowingBuiltins
    @classmethod
    async def get_one(
        cls,
        id: int,
        _config: Optional[MinosConfig] = None,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
    ) -> T:
        """Get one aggregate based on an identifier.

        :param id: aggregate identifier.
        :param _config: Current minos config.
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

        instance = cls.from_avro_bytes(entry.data, id=entry.aggregate_id, version=entry.version)
        instance._broker = _broker
        instance._repository = _repository
        return instance

    @classmethod
    async def create(
        cls,
        *args,
        _config: Optional[MinosConfig] = None,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        **kwargs,
    ) -> T:
        """Create a new ``Aggregate`` instance.

        :param args: Additional positional arguments.
        :param _config: Current minos config.
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

        entry = await _repository.insert(instance)

        instance.id = entry.aggregate_id
        instance.version = entry.version

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    @self_or_classmethod
    async def update(
        self_or_cls,
        id: Optional[int] = None,
        _config: Optional[MinosConfig] = None,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
        **kwargs,
    ) -> T:
        """Update an existing ``Aggregate`` instance.

        :param id: If the method call is performed from an instance the identifier is ignored, otherwise it is used to
            identify the target instance.
        :param _config: Current minos config.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :param kwargs: Additional named arguments.
        :return: An updated ``Aggregate``  instance.
        """
        if "version" in kwargs:
            raise MinosRepositoryManuallySetAggregateVersionException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )

        if _broker is None:
            _broker = self_or_cls._broker

        if _repository is None:
            _repository = self_or_cls._repository
            if isinstance(_repository, Provide):
                raise MinosRepositoryNonProvidedException("A repository instance is required.")

        if isinstance(self_or_cls, type):
            assert issubclass(self_or_cls, Aggregate)
            instance = await self_or_cls.get_one(id, _repository=_repository)
        else:
            instance = self_or_cls

        # Update model...
        for key, value in kwargs.items():
            setattr(instance, key, value)

        entry = await _repository.update(instance)
        instance.id = entry.aggregate_id
        instance.version = entry.version
        return instance

    async def refresh(self) -> NoReturn:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await type(self).get_one(self.id, _repository=self._repository)
        self._fields |= new.fields

    # noinspection PyMethodParameters,PyShadowingBuiltins
    @self_or_classmethod
    async def delete(
        self_or_cls,
        id: Optional[int] = None,
        _config: Optional[MinosConfig] = None,
        _broker: Optional[MinosBroker] = None,
        _repository: Optional[MinosRepository] = None,
    ):
        """Delete the given aggregate instance.

        :param id: If the method call is performed from an instance the identifier is ignored, otherwise it is used to
            identify the target instance.
        :param _config: Current minos config.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :return: This method does not return anything.
        """
        if _broker is None:
            _broker = self_or_cls._broker

        if _repository is None:
            _repository = self_or_cls._repository
            if isinstance(_repository, Provide):
                raise MinosRepositoryNonProvidedException("A repository instance is required.")

        if isinstance(self_or_cls, type):
            assert issubclass(self_or_cls, Aggregate)
            instance = await self_or_cls.get_one(id, _repository=_repository)
        else:
            instance = self_or_cls

        await _repository.delete(instance)
