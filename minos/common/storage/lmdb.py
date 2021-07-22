"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)
from typing import (
    Any,
    NoReturn,
    Optional,
    Type,
    Union,
)

import lmdb

from ..protocol import (
    MinosAvroDatabaseProtocol,
    MinosBinaryProtocol,
)
from .abc import (
    MinosStorage,
)


class MinosStorageLmdb(MinosStorage):
    """Minos Storage LMDB class"""

    __slots__ = "_env", "_protocol", "_tables"

    # noinspection PyUnusedLocal
    def __init__(
        self, env: lmdb.Environment, protocol: Type[MinosBinaryProtocol] = MinosAvroDatabaseProtocol, **kwargs
    ):
        self._env: lmdb.Environment = env
        self._protocol = protocol
        self._tables = {}

    def add(self, table: str, key: str, value: Any) -> NoReturn:
        """Store a value.

        :param table: Table in which the data is stored.
        :param key: Key that identifies the data.
        :param value: Data to be stored.
        :return: This method does not return anything.
        """
        db_instance = self._get_table(table)
        with self._env.begin(write=True) as txn:
            value_bytes: bytes = self._protocol.encode(value)
            txn.put(key.encode(), value_bytes, db=db_instance)

    def get(self, table: str, key: str) -> Optional[Any]:
        """Get the stored value..

        :param table: Table in which the data is stored.
        :param key: Key that identifies the data.
        :return: The stored value or ``None`` if it's empty.
        """
        db_instance = self._get_table(table)
        with self._env.begin(db=db_instance) as txn:
            value_binary = txn.get(key.encode())
            if value_binary is not None:
                # decode the returned value
                return self._protocol.decode(value_binary)
            return None

    def delete(self, table: str, key: str) -> NoReturn:
        """Delete the stored value.

        :param table: Table in which the data is stored.
        :param key: Key that identifies the data.
        :return: This method does not return anything.
        """
        db_instance = self._get_table(table)
        with self._env.begin(write=True, db=db_instance) as txn:
            txn.delete(key.encode())

    def update(self, table: str, key: str, value: Any) -> NoReturn:
        """Update the stored value.

        :param table: Table in which the data is stored.
        :param key: Key that identifies the data.
        :param value: Data to be stored.
        :return: This method does not return anything.
        """
        db_instance = self._get_table(table)
        with self._env.begin(write=True, db=db_instance) as txn:
            value_bytes: bytes = self._protocol.encode(value)
            txn.put(key.encode(), value_bytes, db=db_instance, overwrite=True)

    def _get_table(self, table: str):
        if table in self._tables:
            return self._tables[table]
        else:
            # create a new table
            self._tables[table] = self._env.open_db(table.encode())
            return self._tables[table]

    @classmethod
    def build(cls, path: Union[str, Path], max_db: int = 10, map_size: int = int(1e9), **kwargs) -> MinosStorageLmdb:
        """Build a new instance.

        :param path: Path in which the database is stored.
        :param max_db: Maximum number of available databases.
        :param map_size: Maximum number of entries to be stored on the database. Default set to 1GB
        :param kwargs: Additional named arguments.
        :return: A ``MinosStorageLmdb`` instance.
        """

        env: lmdb.Environment = lmdb.open(str(path), max_dbs=max_db, map_size=map_size)
        return cls(env, **kwargs)
