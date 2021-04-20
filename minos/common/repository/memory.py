"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from itertools import (
    count,
)
from typing import (
    Optional,
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

    def select(
        self,
        aggregate_id: Optional[int] = None,
        aggregate_name: Optional[str] = None,
        version: Optional[int] = None,
        version_lt: Optional[int] = None,
        version_gt: Optional[int] = None,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_gt: Optional[int] = None,
        *args,
        **kwargs
    ) -> list[MinosRepositoryEntry]:
        """TODO

        :param aggregate_id: TODO
        :param aggregate_name: TODO
        :param version: TODO
        :param version_lt: TODO
        :param version_gt: TODO
        :param id: TODO
        :param id_lt: TODO
        :param id_gt: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """

        def _fn_filter(entry: MinosRepositoryEntry) -> bool:
            if aggregate_id is not None and aggregate_id != entry.aggregate_id:
                return False
            if aggregate_name is not None and aggregate_name != entry.aggregate_name:
                return False
            if version is not None and version != entry.version:
                return False
            if version_lt is not None and version_lt > entry.version:
                return False
            if version_gt is not None and version_gt < entry.version:
                return False
            if id is not None and id != entry.id:
                return False
            if id_lt is not None and id_lt > entry.id:
                return False
            if id_gt is not None and id_gt < entry.id:
                return False

            return True

        iterable = iter(self._storage)
        iterable = filter(_fn_filter, iterable)
        return list(iterable)
