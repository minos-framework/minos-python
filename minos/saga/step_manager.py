"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import time
from typing import (
    Any,
    NoReturn,
    Type,
)

from minos.common import (
    MinosStorage,
    MinosStorageLmdb,
)

from .local_state import (
    MinosLocalState,
)


class MinosSagaStepManager:
    """
    Example of how states are stored:

    """

    def __init__(self, name, uuid: str, db_path: str, storage: Type[MinosStorage] = MinosStorageLmdb):
        self.db_name = "LocalState"
        self.db_path = db_path
        # noinspection PyArgumentList
        self._storage = storage.build(path_db=self.db_path)
        self._local_state = MinosLocalState(storage=self._storage, db_name=self.db_name)
        self.uuid = uuid
        self.saga_name = name
        self._state = {}

    def start(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        structure = {"saga": self.saga_name, "current_step": None, "operations": {}}
        self._local_state.update(self.uuid, structure)

    def create_operation_db(self, step_uuid: str, operation_type: str, name: str = "") -> (bool, str):
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
                self.operation(step_uuid, operation_type, name)
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, error

    def operation(self, step_uuid: str, operation_type: str, name: str = ""):
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

    def operation_response_db(self, step_uuid: str, response: str) -> (bool, str):
        """TODO

        :param step_uuid: TODO
        :param response: TODO
        :return: TODO
        """
        flag = False
        error = ""
        for x in range(2):
            try:
                self.add_response(step_uuid, response)
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, error

    def add_response(self, step_uuid: str, response: str):
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
                self.add_error(step_uuid, str(err))
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, error

    def add_error(self, step_uuid: str, error: str):
        """TODO

        :param step_uuid: TODO
        :param error: TODO
        :return: TODO
        """
        self._state = self._local_state.load_state(self.uuid)
        self._state["operations"][step_uuid]["error"] = error
        self._local_state.update(self.uuid, self._state)

    def get_last_response_db(self) -> (bool, str, str):
        """TODO

        :return: TODO
        """
        flag = False
        response = ""
        error = ""
        for x in range(2):
            try:
                response = self.get_last_response()
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, response, error

    def get_last_response(self) -> str:
        """TODO

        :return: TODO
        """
        self._state = self._local_state.load_state(self.uuid)
        return self._state["operations"][self._state["current_step"]]["response"]

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
