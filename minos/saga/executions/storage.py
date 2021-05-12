"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)
from typing import (
    Type,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    MinosJsonBinaryProtocol,
    MinosStorage,
    MinosStorageLmdb,
)

from ..exceptions import (
    MinosSagaExecutionNotFoundException,
)
from .saga import (
    SagaExecution,
)


class SagaExecutionStorage(object):
    """TODO"""

    def __init__(
        self,
        db_path: Union[Path, str],
        storage_cls: Type[MinosStorage] = MinosStorageLmdb,
        protocol=MinosJsonBinaryProtocol,
        db_name: str = "LocalState",
        **kwargs,
    ):
        self.db_name = db_name

        # FIXME: call storage_cls.build instead of this code.
        import lmdb

        env = lmdb.open(str(db_path), max_dbs=10)
        # noinspection PyArgumentList
        self._storage = storage_cls(env, protocol=protocol, **kwargs)

    def store(self, execution: SagaExecution):
        """TODO

        :param execution: TODO
        :return: TODO
        """
        key = str(execution.uuid)
        value = execution.raw
        self._storage.update(table=self.db_name, key=key, value=value)

    def load(self, key: Union[str, UUID]) -> SagaExecution:
        """TODO

        :param key: TODO
        :return: TODO
        """
        key = str(key)
        value = self._storage.get(table=self.db_name, key=key)
        if value is None:
            raise MinosSagaExecutionNotFoundException(f"The execution identified by {key} was not found.")
        execution = SagaExecution.from_raw(value)
        return execution

    def delete(self, key: Union[str, UUID]):
        """TODO

        :param key: TODO
        :return: TODO
        """
        key = str(key)
        self._storage.delete(table=self.db_name, key=key)
