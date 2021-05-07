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
    Any,
    NoReturn,
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
        SagaContext,
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

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def start(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        structure = {"saga": self.saga_name, "current_step": None, "operations": {}}
        self._local_state.update(self.uuid, structure)

    def create_operation(self, operation):
        """TODO

        :param operation: TODO
        :return: TODO
        """
        self._create_operation_db(operation["id"], operation["type"], operation["name"])

    def _create_operation_db(self, step_uuid: str, operation_type: str, name: str = "") -> (bool, str):
        """TODO

        :param step_uuid: TODO
        :param operation_type: TODO
        :param name: TODO
        :return: TODO
        """
        flag = False
        error = ""
        for x in range(2):
            try:
                self._operation(step_uuid, operation_type, name)
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e

        return flag, error

    def _operation(self, step_uuid: str, operation_type: str, name: str = ""):
        """TODO

        :param step_uuid: TODO
        :param operation_type: TODO
        :param name: TODO
        :return: TODO
        """
        operation = {
            "id": step_uuid,
            "type": operation_type,
            "name": name,
            "status": 0,
            "response": "",
            "error": "",
        }

        self._state = self._local_state.load_state(self.uuid)

        self._state["current_step"] = step_uuid
        self._state["operations"][step_uuid] = operation
        self._local_state.add(self.uuid, self._state)

    def store_operation_response(self, step_uuid: str, response: SagaContext) -> (bool, str):
        """TODO

        :param step_uuid: TODO
        :param response: TODO
        :return: TODO
        """
        flag = False
        error = ""
        for x in range(2):
            try:
                self._add_response(step_uuid, response)
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e

        return flag, error

    def _add_response(self, step_uuid: str, response: SagaContext):
        """TODO

        :param step_uuid: TODO
        :param response: TODO
        :return: TODO
        """
        self._state = self._local_state.load_state(self.uuid)
        self._state["operations"][step_uuid]["response"] = response
        self._local_state.update(self.uuid, self._state)

    def operation_error_db(self, step_uuid: str, err: Exception) -> (bool, str):
        """TODO

        :param step_uuid: TODO
        :param err: TODO
        :return: TODO
        """
        flag = False
        error = ""
        for x in range(2):
            try:
                self._add_error(step_uuid, str(err))
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e

        return flag, error

    def _add_error(self, step_uuid: str, error: str):
        """TODO

        :param step_uuid: TODO
        :param error: TODO
        :return: TODO
        """
        self._state = self._local_state.load_state(self.uuid)
        self._state["operations"][step_uuid]["error"] = error
        self._local_state.update(self.uuid, self._state)

    def get_state(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return self._local_state.load_state(self.uuid)

    def close(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        # self._state = self._local_state.load_state(self.uuid)
        self._local_state.delete_state(self.uuid)
