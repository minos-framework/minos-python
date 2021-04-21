"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import annotations

from operator import attrgetter
from typing import (
    NoReturn,
    Optional,
)

from ..exceptions import (
    MinosRepositoryAggregateNotFoundException,
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryManuallySetAggregateIdException,
    MinosRepositoryManuallySetAggregateVersionException,
)
from ..repository import (
    MinosRepository,
    MinosRepositoryAction,
)
from .abc import MinosModel


# noinspection PyPep8Naming
class class_or_instancemethod(classmethod):
    """Decorator to use the same method as instance or as class based on the caller."""

    # noinspection PyMethodOverriding
    def __get__(self, instance, type_):
        # noinspection PyUnresolvedReferences
        get = super().__get__ if instance is None else self.__func__.__get__
        # noinspection PyArgumentList
        return get(instance, type_)


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
    def get_namespace(cls) -> str:
        """Compute the current class namespace.

        :return: An string object.
        """
        return f"{cls.__module__}.{cls.__qualname__}"

    @classmethod
    async def get(cls, ids: list[int], _broker: str = None, _repository: MinosRepository = None) -> list[Aggregate]:
        """Get a sequence of aggregates based on a list of identifiers.

        :param ids: list of identifiers.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :return: A list of aggregate instances.
        """
        # noinspection PyShadowingBuiltins
        return [await cls.get_one(id, _broker, _repository) for id in ids]

    # noinspection PyShadowingBuiltins
    @classmethod
    async def get_one(cls, id: int, _broker: str = None, _repository: MinosRepository = None) -> Aggregate:
        """Get one aggregate based on an identifier.

        :param id: aggregate identifier.
        :param _broker: Broker to be set to the aggregates.
        :param _repository: Repository to be set to the aggregate.
        :return: A list of aggregate instances.
        :return: An aggregate instance.
        """

        entries = await _repository.select(aggregate_name=cls.get_namespace(), aggregate_id=id)
        if not len(entries):
            raise MinosRepositoryAggregateNotFoundException(f"Not found any entries for the {repr(id)} id.")

        entry = max(entries, key=attrgetter("version"))
        if entry.action == MinosRepositoryAction.DELETE:
            raise MinosRepositoryDeletedAggregateException(f"The {id} id points to an already deleted aggregate.")

        instance = cls.from_avro_bytes(entry.data)
        instance._broker = _broker
        instance._repository = _repository
        return instance

    @classmethod
    async def create(cls, *args, _broker: str = None, _repository: MinosRepository = None, **kwargs) -> Aggregate:
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
            _broker = "MinosBaseBroker()"

        if _repository is None:
            raise Exception()

        instance = cls(0, 0, *args, _broker=_broker, _repository=_repository, **kwargs)

        await _repository.insert(instance)

        return instance

    # noinspection PyMethodParameters,PyShadowingBuiltins
    @class_or_instancemethod
    async def update(self_or_cls, id: int = None, _repository: MinosRepository = None, **kwargs) -> Aggregate:
        """Update an existing ``Aggregate`` instance.

        :param id: If the method call is performed from an instance the identifier is ignored, otherwise it is used to
            identify the target instance.
        :param _repository: Repository to be set to the aggregate.
        :param kwargs: Additional named arguments.
        :return: An updated ``Aggregate``  instance.
        """
        if "version" in kwargs:
            raise Exception()

        if isinstance(self_or_cls, type):
            assert issubclass(self_or_cls, Aggregate)
            instance = await self_or_cls.get_one(id, _repository=_repository)
        else:
            instance = self_or_cls

        if _repository is None:
            _repository = instance._repository

        # Update model...
        for key, value in kwargs.items():
            setattr(instance, key, value)

        await _repository.update(instance)

        return instance

    async def refresh(self) -> NoReturn:
        """Refresh the state of the given instance.

        :return: This method does not return anything.
        """
        new = await type(self).get_one(self.id, _repository=self._repository)
        self._fields |= new.fields

    # noinspection PyMethodParameters,PyShadowingBuiltins
    @class_or_instancemethod
    async def delete(self_or_cls, id: Optional[int] = None, _repository: MinosRepository = None):
        """Delete the given aggregate instance.

        :param id: If the method call is performed from an instance the identifier is ignored, otherwise it is used to
            identify the target instance.
        :param _repository: Repository to be set to the aggregate.
        :return: This method does not return anything.
        """
        if isinstance(self_or_cls, type):
            assert issubclass(self_or_cls, Aggregate)
            instance = await self_or_cls.get_one(id, _repository=_repository)
        else:
            instance = self_or_cls

        if _repository is None:
            _repository = instance._repository

        await _repository.delete(instance)
