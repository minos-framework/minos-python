"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    NoReturn,
    Type,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    MinosConfig,
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
    """Saga Execution Storage class."""

    def __init__(
        self,
        storage_cls: Type[MinosStorage] = MinosStorageLmdb,
        protocol=MinosJsonBinaryProtocol,
        db_name: str = "LocalState",
        **kwargs,
    ):
        self.db_name = db_name
        self._storage = storage_cls.build(protocol=protocol, **kwargs)

    @classmethod
    def from_config(cls, *args, config: MinosConfig, **kwargs) -> SagaExecutionStorage:
        """Build an instance from config.

        :param args: Additional positional arguments.
        :param config: Config instance.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaExecutionStorage`` instance.
        """
        # noinspection PyProtectedMember
        return cls(*args, **(config.saga.storage._asdict() | kwargs))

    def store(self, execution: SagaExecution) -> NoReturn:
        """Store an execution.

        :param execution: Execution to be stored.
        :return: This method does not return anything.
        """
        key = str(execution.uuid)
        value = execution.raw
        self._storage.update(table=self.db_name, key=key, value=value)

    def load(self, key: Union[str, UUID]) -> SagaExecution:
        """Load the saga execution stored on the given key.

        :param key: The key to identify the execution.
        :return: A ``SagaExecution`` instance.
        """
        key = str(key)
        value = self._storage.get(table=self.db_name, key=key)
        if value is None:
            raise MinosSagaExecutionNotFoundException(f"The execution identified by {key} was not found.")
        execution = SagaExecution.from_raw(value)
        return execution

    def delete(self, key: Union[SagaExecution, str, UUID]) -> NoReturn:
        """Delete the reference of the given key.

        :param key: Execution key to be deleted.
        :return: This method does not return anything.
        """
        if isinstance(key, SagaExecution):
            key = key.uuid

        key = str(key)
        self._storage.delete(table=self.db_name, key=key)
