"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

import typing as t
from pathlib import (
    Path,
)

import lmdb

from ..protocol import (
    MinosAvroValuesDatabase,
    MinosBinaryProtocol,
)
from .abc import (
    MinosStorage,
)


class MinosStorageLmdb(MinosStorage):
    """Minos Storage LMDB class"""

    __slots__ = "_env", "_protocol", "_tables"

    def __init__(self, env: lmdb.Environment, protocol: t.Type[MinosBinaryProtocol] = MinosAvroValuesDatabase):
        self._env: lmdb.Environment = env
        self._protocol = protocol
        self._tables = {}

    @property
    def env(self) -> lmdb.Environment:
        """Env getter.

        :return: An ``lmdb.Environment`` instance.
        """
        return self._env

    def add(self, table: str, key: str, value: t.Any) -> t.NoReturn:
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

    def get(self, table: str, key: str) -> t.Optional[t.Any]:
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

    def delete(self, table: str, key: str) -> t.NoReturn:
        """Delete the stored value.

        :param table: Table in which the data is stored.
        :param key: Key that identifies the data.
        :return: This method does not return anything.
        """
        db_instance = self._get_table(table)
        with self._env.begin(write=True, db=db_instance) as txn:
            txn.delete(key.encode())

    def update(self, table: str, key: str, value: t.Any) -> t.NoReturn:
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
    def build(cls, path_db: t.Union[str, Path], max_db: int = 10, **kwargs) -> MinosStorageLmdb:
        """Build a new instance.

        :param path_db: Path in which the database is stored.
        :param max_db: Maximum number of available databases.
        :param kwargs: Additional named arguments.
        :return: A ``MinosStorageLmdb`` instance.
        """

        env: lmdb.Environment = lmdb.open(str(path_db), max_dbs=max_db)
        return cls(env, **kwargs)
