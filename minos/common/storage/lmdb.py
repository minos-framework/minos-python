import lmdb
import typing as t

from minos.common.protocol.avro import MinosAvroValuesDatabase
from minos.common.storage.abstract import MinosStorage


def _encode_values(value: t.Any) -> bytes:
    return MinosAvroValuesDatabase.encode(value)


def _decode_values(value: t.Any) -> bytes:
    return MinosAvroValuesDatabase.decode(value)


class MinosStorageLmdb(MinosStorage):
    __slots__ = "_env", "_tables"

    def __init__(self, env: lmdb.Environment):
        self._env: lmdb.Environment = env
        self._tables = {}

    @property
    def env(self) -> lmdb.Environment:
        return self._env

    def add(self, table: str, key: str, value: t.Any) -> t.NoReturn:
        db_instance = self._get_table(table)
        with self._env.begin(write=True) as txn:
            value_bytes: bytes = _encode_values(value)
            txn.put(key.encode(), value_bytes, db=db_instance)

    def get(self, table: str, key) -> t.Union[None, t.Any]:
        db_instance = self._get_table(table)
        with self._env.begin(db=db_instance) as txn:
            value_binary = txn.get(key.encode())
            if value_binary is not None:
                # decode the returned value
                return _decode_values(value_binary)
            return None

    def delete(self, table: str, key: str) -> t.NoReturn:
        db_instance = self._get_table(table)
        with self._env.begin(write=True, db=db_instance) as txn:
            txn.delete(key.encode())

    def update(self, table: str, key: str, value: t.Any) -> t.NoReturn:
        db_instance = self._get_table(table)
        with self._env.begin(write=True, db=db_instance) as txn:
            value_bytes: bytes = _encode_values(value)
            txn.put(key.encode(), value_bytes, db=db_instance, overwrite=True)

    def _get_table(self, table: str) -> lmdb._Database:
        if table in self._tables:
            return self._tables[table]
        else:
            # create a new table
            self._tables[table] = self._env.open_db(table.encode())
            return self._tables[table]

    @staticmethod
    def build(path_db: str, max_db: int = 10) -> "MinosStorageLmdb":
        """
        prepare the database initialization
        """

        env: lmdb.Environment = lmdb.open(path_db, max_dbs=max_db)
        return MinosStorageLmdb(env)
