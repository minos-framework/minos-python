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
    TYPE_CHECKING,
    Type,
    Union,
)

from minos.common import (
    MinosStorage,
    MinosStorageLmdb,
)

from .local_state import (
    MinosLocalState,
)

if TYPE_CHECKING:
    from .executions import (
        SagaExecution,
    )


class MinosSagaStorage:
    """
    Example of how states are stored:

    """

    def __init__(
        self,
        name: str,
        uuid: str,
        db_path: Union[Path, str] = "./db.lmdb",
        storage_cls: Type[MinosStorage] = MinosStorageLmdb,
    ):
        self.uuid = uuid
        self.saga_name = name
        self._local_state = MinosLocalState(storage_cls=storage_cls, db_path=db_path, db_name="LocalState")
        self._state = {}

    @classmethod
    def from_execution(cls, execution: SagaExecution, *args, **kwargs) -> MinosSagaStorage:
        """TODO

        :param execution: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        return cls(execution.definition.name, str(execution.uuid), *args, **kwargs)
