"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
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

    def __init__(self, name, uuid: str, db_path: str, storage: MinosStorage = MinosStorageLmdb):
        self.db_name = "LocalState"
        self.db_path = db_path
        self._storage = storage.build(path_db=self.db_path)
        self._local_state = MinosLocalState(storage=self._storage, db_name=self.db_name)
        self.uuid = uuid
        self.saga_name = name
        self._state = {}

    def start(self):
        structure = {"saga": self.saga_name, "current_step": None, "operations": {}}
        self._local_state.update(self.uuid, structure)

    def operation(self, step_uuid: str, type: str, name: str = ""):
        operation = {
            "id": step_uuid,
            "type": type,
            "name": name,
            "status": 0,
            "response": "",
            "error": "",
        }

        self._state = self._local_state.load_state(self.uuid)

        self._state["current_step"] = step_uuid
        self._state["operations"][step_uuid] = operation
        self._local_state.add(self.uuid, self._state)

    def add_response(self, step_uuid: str, response: str):
        self._state = self._local_state.load_state(self.uuid)
        self._state["operations"][step_uuid]["response"] = response
        self._local_state.update(self.uuid, self._state)

    def add_error(self, step_uuid: str, error: str):
        self._state = self._local_state.load_state(self.uuid)
        self._state["operations"][step_uuid]["error"] = error
        self._local_state.update(self.uuid, self._state)

    def get_last_response(self):
        self._state = self._local_state.load_state(self.uuid)
        return self._state["operations"][self._state["current_step"]]["response"]

    def get_state(self):
        return self._local_state.load_state(self.uuid)

    def close(self):
        # self._state = self._local_state.load_state(self.uuid)
        self._local_state.delete_state(self.uuid)
