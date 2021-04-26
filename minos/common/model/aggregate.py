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
    NoReturn,
    Optional,
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
from ..repository import (
    MinosRepository,
    MinosRepositoryAction,
)
from .abc import (
    MinosModel,
)


class Aggregate(MinosModel):
    """Base aggregate class."""

    id: int
    version: int

    # FIXME: The ``broker`` attribute should be a reference to a ``MinosBaseBroker`` class instance.
    # noinspection PyShadowingBuiltins
    def __init__(
        self, id: int, version: int, *args, _broker: str = None, _repository: MinosRepository = None, **kwargs,
    ):

        super().__init__(id, version, *args, **kwargs)
        self._broker = _broker
        self._repository = _repository

    @classmethod
    async def get(
        cls, ids: list[int], _config: MinosConfig = None, _broker: str = None, _repository: MinosRepository = None
    ) -> list[Aggregate]:
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
        cls, id: int, _config: MinosConfig = None, _broker: str = None, _repository: MinosRepository = None
    ) -> Aggregate:
        """Get one aggregate based on an identifier.

        :param id: aggregate identifier.
        :param _config: Current minos config.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :return: A list of aggregate instances.
        :return: An aggregate instance.
        """
        if _repository is None:
            _repository = cls._build_repository(_config)

        # noinspection PyTypeChecker
        entries = await _repository.select(aggregate_name=cls.classname, aggregate_id=id)
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
        cls, *args, _config: MinosConfig = None, _broker: str = None, _repository: MinosRepository = None, **kwargs
    ) -> Aggregate:
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
            _broker = "MinosBaseBroker()"

        if _repository is None:
            _repository = cls._build_repository(_config)

        instance = cls(0, 0, *args, _broker=_broker, _repository=_repository, **kwargs)

        entry = await _repository.insert(instance)

        instance.id = entry.aggregate_id
        instance.version = entry.version

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    @self_or_classmethod
    async def update(
        self_or_cls, id: int = None, _config: MinosConfig = None, _repository: MinosRepository = None, **kwargs
    ) -> Aggregate:
        """Update an existing ``Aggregate`` instance.

        :param id: If the method call is performed from an instance the identifier is ignored, otherwise it is used to
            identify the target instance.
        :param _config: Current minos config.
        :param _repository: Repository to be set to the aggregate.
        :param kwargs: Additional named arguments.
        :return: An updated ``Aggregate``  instance.
        """
        if "version" in kwargs:
            raise MinosRepositoryManuallySetAggregateVersionException(
                f"The version must be computed internally on the repository. Obtained: {kwargs['version']}"
            )

        if _repository is None:
            _repository = self_or_cls._build_repository(_config)

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
        self_or_cls, id: Optional[int] = None, _config: MinosConfig = None, _repository: MinosRepository = None
    ):
        """Delete the given aggregate instance.

        :param id: If the method call is performed from an instance the identifier is ignored, otherwise it is used to
            identify the target instance.
        :param _config: Current minos config.
        :param _repository: Repository to be set to the aggregate.
        :return: This method does not return anything.
        """
        if _repository is None:
            _repository = self_or_cls._build_repository(_config)

        if isinstance(self_or_cls, type):
            assert issubclass(self_or_cls, Aggregate)
            instance = await self_or_cls.get_one(id, _repository=_repository)
        else:
            instance = self_or_cls

        await _repository.delete(instance)

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _build_repository(self_or_cls, config: MinosConfig) -> MinosRepository:
        repository = None

        if not isinstance(self_or_cls, type):
            repository = self_or_cls._repository

        if repository is None:
            # FIXME: In the future, this could be parameterized.
            from ..repository import PostgreSqlMinosRepository

            repository = PostgreSqlMinosRepository.from_config(already_setup=True, config=config)

        if repository is None:
            raise MinosRepositoryNonProvidedException("A repository instance is required.")

        return repository
