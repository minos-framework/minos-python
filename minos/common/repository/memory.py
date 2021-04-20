"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from itertools import (
    count,
)

from .abc import (
    MinosRepository,
)
from .entries import (
    MinosRepositoryAction,
    MinosRepositoryEntry,
)


def _int_generator():
    yield from count()


class MinosInMemoryRepository(MinosRepository):
    """TODO"""

    def __init__(self):
        self._storage = list()
        self._id_generator = _int_generator()

    def _set_id(self, value):
        value.id = next(self._id_generator)
        return value

    def insert(self, value: MinosRepositoryEntry):
        """TODO

        :param value: TODO
        :return: TODO
        """
        self._set_id(value)
        value.action = MinosRepositoryAction.INSERT
        self._storage.append(value)

    def update(self, value: MinosRepositoryEntry):
        """TODO

        :param value: TODO
        :return: TODO
        """
        self._set_id(value)
        value.action = MinosRepositoryAction.UPDATE
        self._storage.append(value)

    def delete(self, value: MinosRepositoryEntry):
        """TODO

        :param value: TODO
        :return: TODO
        """
        self._set_id(value)
        value.action = MinosRepositoryAction.DELETE
        self._storage.append(value)

    def select(self, *args, **kwargs) -> list[MinosRepositoryEntry]:
        """TODO

        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        return self._storage
