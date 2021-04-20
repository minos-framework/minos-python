"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from operator import (
    attrgetter,
)

from ..exceptions import (
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryEntryNotFoundException,
)
from ..repository import (
    MinosRepository,
    MinosRepositoryAction,
    MinosRepositoryEntry,
)
from .abc import (
    MinosModel,
)


# noinspection PyPep8Naming
class class_or_instancemethod(classmethod):
    """TODO"""

    # noinspection PyMethodOverriding
    def __get__(self, instance, type_):
        # noinspection PyUnresolvedReferences
        get = super().__get__ if instance is None else self.__func__.__get__
        # noinspection PyArgumentList
        return get(instance, type_)


class Aggregate(MinosModel):
    """TODO"""

    id: int
    version: int

    # FIXME: The ``broker`` attribute should be a reference to a ``MinosBaseBroker`` class instance.
    def __init__(
        self,
        id: int,
        version: int,
        *args,
        _broker: str = None,
        _repository: MinosRepository = None,
        **kwargs,
    ):

        super().__init__(id, version, *args, **kwargs)
        self._broker = _broker
        self._repository = _repository

    @classmethod
    def _get_namespace(cls) -> str:
        """TODO

        :return: TODO
        """
        return cls.__name__

    @classmethod
    def get(cls, ids: list[int], _broker: str = None, _repository: MinosRepository = None) -> list["Aggregate"]:
        """TODO

        :param ids: TODO
        :param _broker: TODO
        :param _repository: TODO
        :return: TODO
        """
        return [cls.get_one(id, _broker, _repository) for id in ids]

    @classmethod
    def get_one(cls, id: int, _broker: str = None, _repository: MinosRepository = None) -> "Aggregate":
        """TODO

        :param id: TODO
        :param _broker: TODO
        :param _repository: TODO
        :return: TODO
        """

        entries = _repository.select(aggregate_name=cls._get_namespace(), aggregate_id=id)
        if not len(entries):
            raise MinosRepositoryEntryNotFoundException("TODO")

        entry = max(entries, key=attrgetter("version"))
        if entry.action == MinosRepositoryAction.DELETE:
            raise MinosRepositoryDeletedAggregateException("TODO")

        instance = cls.from_avro_bytes(entry.data)
        instance._broker = _broker
        instance._repository = _repository
        return instance

    @classmethod
    def create(cls, *args, _broker: str = None, _repository: MinosRepository = None, **kwargs) -> "Aggregate":
        """TODO

        :param args: TODO
        :param _broker: TODO
        :param _repository: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if "id" in kwargs:
            raise Exception()

        if "version" in kwargs:
            raise Exception()

        if _broker is None:
            _broker = "MinosBaseBroker()"

        if _repository is None:
            raise Exception()

        id = _repository.generate_aggregate_id(cls._get_namespace())

        instance = cls(id, 0, *args, _broker=_broker, _repository=_repository, **kwargs)

        entry = MinosRepositoryEntry.from_aggregate(instance)
        _repository.insert(entry)

        return instance

    # noinspection PyMethodParameters
    @class_or_instancemethod
    def update(self_or_cls, id: int = None, _repository: MinosRepository = None, **kwargs) -> "Aggregate":
        """TODO

        :param id: TODO
        :param _repository: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if "version" in kwargs:
            raise Exception()

        if isinstance(self_or_cls, type):
            assert issubclass(self_or_cls, Aggregate)
            instance = self_or_cls.get_one(id, _repository=_repository)
        else:
            instance = self_or_cls

        if _repository is None:
            _repository = instance._repository

        # Update model...
        for key, value in kwargs.items():
            setattr(instance, key, value)

        instance.version = _repository.get_next_version_id(instance._get_namespace(), instance.id)
        entry = MinosRepositoryEntry.from_aggregate(instance)
        _repository.update(entry)

        return instance

    def refresh(self):
        """TODO"""
        new = type(self).get_one(self.id, _repository=self._repository)
        self._fields |= new.fields

    # noinspection PyMethodParameters
    @class_or_instancemethod
    def delete(self_or_cls, id: int = None, _repository: MinosRepository = None):
        """TODO

        :param id: TODO
        :param _repository: TODO
        :return: TODO
        """
        if isinstance(self_or_cls, type):
            assert issubclass(self_or_cls, Aggregate)
            instance = self_or_cls.get_one(id, _repository=_repository)
        else:
            instance = self_or_cls

        repository = instance._repository

        instance.version = repository.get_next_version_id(instance._get_namespace(), instance.id)
        entry = MinosRepositoryEntry.from_aggregate(instance)
        repository.delete(entry)
