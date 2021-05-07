"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)
from typing import (
    Any,
    Type,
    Union,
)

from minos.common import (
    MinosStorage,
)


class MinosLocalState:
    """TODO"""

    def __init__(self, db_path: Union[Path, str], storage_cls: Type[MinosStorage], db_name: str):
        self.db_path = db_path
        self.db_name = db_name
        # noinspection PyArgumentList
        self._storage = storage_cls.build(path_db=str(self.db_path))

    def update(self, key: str, value: Any):
        """TODO

        :param key: TODO
        :param value: TODO
        :return: TODO
        """
        actual_state = self._storage.get(table=self.db_name, key=key)
        if actual_state is not None:
            self._storage.update(table=self.db_name, key=key, value=value)
        else:
            self._storage.add(table=self.db_name, key=key, value=value)

    def add(self, key: str, value: str):
        """TODO

        :param key: TODO
        :param value: TODO
        :return: TODO
        """
        self._storage.add(table=self.db_name, key=key, value=value)

    def get(self, key: str):
        """TODO

        :param key: TODO
        :return: TODO
        """
        actual_state = self._storage.get(table=self.db_name, key=key)
        return actual_state

    def delete(self, key: str):
        """TODO

        :param key: TODO
        :return: TODO
        """
        self._storage.delete(table=self.db_name, key=key)
