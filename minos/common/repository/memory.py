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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def _set_id(self, entry: MinosRepositoryEntry):
        entry.id = next(self._id_generator)
        return entry

    def insert(self, entry: MinosRepositoryEntry):
        """TODO

        :param entry: TODO
        :return: TODO
        """
        self._set_id(entry)
        entry.action = MinosRepositoryAction.INSERT
        self._storage.append(entry)

    def update(self, entry: MinosRepositoryEntry):
        """TODO

        :param entry: TODO
        :return: TODO
        """
        self._set_id(entry)
        entry.action = MinosRepositoryAction.UPDATE
        self._storage.append(entry)

    def delete(self, entry: MinosRepositoryEntry):
        """TODO

        :param entry: TODO
        :return: TODO
        """
        self._set_id(entry)
        entry.action = MinosRepositoryAction.DELETE
        self._storage.append(entry)

    def select(
        self,
        aggregate_id: Optional[int] = None,
        aggregate_name: Optional[str] = None,
        version: Optional[int] = None,
        version_lt: Optional[int] = None,
        version_gt: Optional[int] = None,
        version_le: Optional[int] = None,
        version_ge: Optional[int] = None,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_le: Optional[int] = None,
        id_ge: Optional[int] = None,
        *args,
        **kwargs
    ) -> list[MinosRepositoryEntry]:
        """TODO

        :param aggregate_id: TODO
        :param aggregate_name: TODO
        :param version: TODO
        :param version_lt: TODO
        :param version_gt: TODO
        :param version_le: TODO
        :param version_ge: TODO
        :param id: TODO
        :param id_lt: TODO
        :param id_gt: TODO
        :param id_le: TODO
        :param id_ge: TODO
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
            if version_lt is not None and version_lt <= entry.version:
                return False
            if version_gt is not None and version_gt >= entry.version:
                return False
            if version_le is not None and version_le < entry.version:
                return False
            if version_ge is not None and version_ge > entry.version:
                return False
            if id is not None and id != entry.id:
                return False
            if id_lt is not None and id_lt <= entry.id:
                return False
            if id_gt is not None and id_gt >= entry.id:
                return False
            if id_le is not None and id_le < entry.id:
                return False
            if id_ge is not None and id_ge > entry.id:
                return False

            return True

        iterable = iter(self._storage)
        iterable = filter(_fn_filter, iterable)
        return list(iterable)

    def generate_aggregate_id(self, aggregate_name: str) -> int:
        """TODO

        :param aggregate_name: TODO
        :return: TODO
        """
        iterable = iter(self._storage)
        iterable = filter(lambda entry: entry.aggregate_name == aggregate_name, iterable)
        return len(list(iterable))

    def get_next_version_id(self, aggregate_name: str, aggregate_id: int) -> int:
        """TODO

        :param aggregate_name: TODO
        :param aggregate_id: TODO
        :return: TODO
        """
        iterable = iter(self._storage)
        iterable = filter(
            lambda entry: entry.aggregate_name == aggregate_name and entry.aggregate_id == aggregate_id, iterable,
        )
        return len(list(iterable))
