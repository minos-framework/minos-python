"""This module contains the implementation of the lmdb client."""

from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)
from typing import (
    Any,
    Optional,
    Union,
)

from lmdb import (
    Environment,
)

from minos.common import (
    DatabaseClient,
    DatabaseOperation,
    MinosBinaryProtocol,
    MinosJsonBinaryProtocol,
)

from .operations import (
    LmdbDatabaseOperation,
    LmdbDatabaseOperationType,
)

not_found_sentinel = object()


class LmdbDatabaseClient(DatabaseClient):
    """Lmdb Database Client class."""

    _environment: Optional[Environment]

    def __init__(
        self,
        path: Optional[Union[str, Path]] = None,
        max_tables: int = 100,
        map_size: int = int(1e9),
        protocol: type[MinosBinaryProtocol] = MinosJsonBinaryProtocol,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if path is None:
            path = ".lmdb"

        self._path = path
        self._max_tables = max_tables
        self._map_size = map_size
        self._protocol = protocol
        self._tables = {}

        self._prefetched = not_found_sentinel

        self._environment = None

    async def _setup(self) -> None:
        await super()._setup()
        self._create_environment()

    async def _destroy(self) -> None:
        await super()._destroy()
        self._close_environment()

    def _create_environment(self) -> None:
        self._environment = Environment(str(self._path), max_dbs=self._max_tables, map_size=self._map_size)

    def _close_environment(self) -> None:
        if self._environment is not None:
            self._environment.close()

    async def _reset(self, **kwargs) -> None:
        self._prefetched = None
        self._environment.sync()

    async def _fetch_all(self, *args, **kwargs) -> Any:
        if self._prefetched is not_found_sentinel:
            return
        prefetched = self._prefetched
        self._prefetched = not_found_sentinel
        yield prefetched

    async def _execute(self, operation: DatabaseOperation) -> None:
        if not isinstance(operation, LmdbDatabaseOperation):
            raise ValueError(f"The operation must be a {LmdbDatabaseOperation!r} instance. Obtained: {operation!r}")

        mapper = {
            LmdbDatabaseOperationType.CREATE: self._create,
            LmdbDatabaseOperationType.READ: self._read,
            LmdbDatabaseOperationType.UPDATE: self._update,
            LmdbDatabaseOperationType.DELETE: self._delete,
        }

        fn = mapper[operation.type_]

        fn(table=operation.table, key=operation.key, value=operation.value)

    # noinspection PyUnusedLocal
    def _create(self, table: str, key: str, value: Any, **kwargs) -> None:
        table = self._get_table(table)
        with self._environment.begin(write=True) as transaction:
            encoded = self._protocol.encode(value)
            transaction.put(key.encode(), encoded, db=table)

    # noinspection PyUnusedLocal
    def _read(self, table: str, key: str, **kwargs):
        table = self._get_table(table)
        with self._environment.begin(db=table) as transaction:
            value = transaction.get(key.encode(), default=not_found_sentinel)
            if value is not not_found_sentinel:
                value = self._protocol.decode(value)

        self._prefetched = value

    # noinspection PyUnusedLocal
    def _delete(self, table: str, key: str, **kwargs) -> None:
        table = self._get_table(table)
        with self._environment.begin(write=True, db=table) as transaction:
            transaction.delete(key.encode())

    # noinspection PyUnusedLocal
    def _update(self, table: str, key: str, value: Any, **kwargs) -> None:
        table = self._get_table(table)
        with self._environment.begin(write=True, db=table) as transaction:
            encoded = self._protocol.encode(value)
            transaction.put(key.encode(), encoded, db=table, overwrite=True)

    def _get_table(self, table: str) -> Any:
        if table not in self._tables:
            self._tables[table] = self._environment.open_db(table.encode())
        return self._tables[table]
