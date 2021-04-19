"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import NoReturn

from minos.common.model.abc.model import MinosModel


class class_or_instancemethod(classmethod):
    def __get__(self, instance, type_):
        descr_get = super().__get__ if instance is None else self.__func__.__get__
        return descr_get(instance, type_)


class Aggregate(MinosModel):
    """TODO"""

    id: int
    version: int

    # FIXME: The ``broker`` attribute should be a reference to a ``MinosBaseBroker`` class instance.
    # FIXME: The ``repository`` attribute should be a reference to a ``MinosRepository`` class instance.
    def __init__(self, id: int, version: int, *args, broker: str = None, repository: str = None, **kwargs):
        super().__init__(id, version, *args, **kwargs)
        self._broker = broker
        self._repository = repository

    @classmethod
    def get(cls, ids: list[int]) -> list["Aggregate"]:
        """TODO

        :param ids: TODO
        :return: TODO
        """

        # Get multiple model instances...

        pass

    @classmethod
    def get_one(cls, id: int) -> "Aggregate":
        """TODO

        :param id: TODO
        :return: TODO
        """

        # Get one model instance...

        pass

    @classmethod
    def create(cls, *args, **kwargs) -> "Aggregate":
        """TODO

        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if "id" in kwargs:
            raise Exception()

        if "version" in kwargs:
            raise Exception()

        return cls(id=0, version=0, *args, broker="MinosBaseBroker()", repository="PostgreSqlRepository()", **kwargs)

    @class_or_instancemethod
    def update(self_or_cls, id: int = None, **kwargs) -> "Aggregate":
        """TODO

        :param identifier:TODO
        :param kwargs:TODO
        :return: TODO
        """
        if "version" in kwargs:
            raise Exception()

        if isinstance(self_or_cls, type):
            instance = self_or_cls.get_one(id)
        else:
            instance = self_or_cls

        # Update model...
        for key, value in kwargs.items():
            setattr(instance, key, value)
        instance.version += 1

        return instance

    @class_or_instancemethod
    def delete(self_or_cls, id: int = None) -> NoReturn:
        """TODO

        :param id: TODO
        :return: TODO
        """
        if not isinstance(self_or_cls, type):
            id = self_or_cls.id

        # Delete model...

        return
