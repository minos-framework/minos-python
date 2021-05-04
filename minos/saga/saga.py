# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import asyncio
import inspect
import time
import typing as t
import uuid

from minos.common import (
    MinosStorage,
    MinosStorageLmdb,
)

from .abstract import MinosBaseSagaBuilder
from .exceptions import MinosSagaException


class MinosLocalState:
    def __init__(self, storage: MinosStorage, db_name: str):
        self._storage = storage
        self.db_name = db_name

    def update(self, key: str, value: str):
        actual_state = self._storage.get(self.db_name, key)
        if actual_state is not None:
            self._storage.update(self.db_name, key, value)
        else:
            self._storage.add(self.db_name, key, value)

    def add(self, key: str, value: str):
        self._storage.add(self.db_name, key, value)

    def load_state(self, key: str):
        actual_state = self._storage.get(self.db_name, key)
        return actual_state

    def delete_state(self, key: str):
        self._storage.delete(self.db_name, key)


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


def _invokeParticipant(name):
    if name == "Shipping":
        raise MinosSagaException("invokeParticipantTest exception")

    return "_invokeParticipant Response"


def _withCompensation(name):
    return "_withCompensation Response"


class Saga(MinosBaseSagaBuilder):
    def __init__(
        self,
        name,
        db_path: str = "./db.lmdb",
        step_manager: MinosSagaStepManager = MinosSagaStepManager,
        loop: asyncio.AbstractEventLoop = None,
    ):
        self.saga_name = name
        self.uuid = str(uuid.uuid4())
        self.saga_process = {
            "name": self.saga_name,
            "id": self.uuid,
            "steps": [],
            "current_compensations": [],
        }
        self._step_manager = step_manager(self.saga_name, self.uuid, db_path)
        self.loop = loop or asyncio.get_event_loop()
        self._response = ""

    def get_db_state(self):
        return self._step_manager.get_state()

    def start(self):
        return self

    def step(self):
        self.saga_process["steps"].append([])
        return self

    def callback_function_call(self, func, response):
        task = func(response)
        if inspect.isawaitable(task):
            result = self.loop.run_until_complete(task)
            return result
        else:
            return task

    def _createOperationDB(self, id, type, name=""):
        flag = False
        error = ""
        for x in range(2):
            try:
                self._step_manager.operation(id, type, name)
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, error

    def _operationResponseDB(self, id, response):
        flag = False
        error = ""
        for x in range(2):
            try:
                self._step_manager.add_response(id, response)
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, error

    def _operationErrorDB(self, id, err):
        flag = False
        error = ""
        for x in range(2):
            try:
                self._step_manager.add_error(id, str(err))
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, error

    def _getLastResponseDB(self):
        flag = False
        response = ""
        error = ""
        for x in range(2):
            try:
                response = self._step_manager.get_last_response()
                flag = True
                error = ""
                break
            except Exception as e:  # pragma: no cover
                error = e
                time.sleep(0.5)

        return flag, response, error

    def _invokeParticipant(self, operation):
        response = None

        # Add current operation to lmdb
        (db_operation_flag, db_operation_error,) = self._createOperationDB(
            operation["id"], operation["type"], operation["name"]
        )
        # if the DB was updated
        if db_operation_flag:
            try:
                response = _invokeParticipant(operation["name"])
            except MinosSagaException as error:
                self._operationErrorDB(operation["id"], error)
                raise error

            # Add response of current operation to lmdb
            (db_op_response_flag, db_op_response_error,) = self._operationResponseDB(operation["id"], response)

            # if the DB was updated with the response of previous operation
            if db_op_response_flag:
                if operation["callback"] is not None:
                    func = operation["callback"]
                    callback_id = str(uuid.uuid4())

                    (db_op_callback_flag, db_op_callback_error,) = self._createOperationDB(
                        callback_id, "invokeParticipant_callback", operation["name"]
                    )
                    # if the DB was updated
                    if db_op_callback_flag:
                        try:
                            response = self.callback_function_call(func, self._response)
                        except MinosSagaException as error:
                            self._operationErrorDB(callback_id, error)
                            raise error

                        # Add response of current operation to lmdb
                        (db_op_callback_response_flag, db_op_callback_response_error,) = self._operationResponseDB(
                            callback_id, response
                        )

                        # If the database could not be updated
                        if not db_op_callback_response_flag:
                            self._operationErrorDB(callback_id, db_op_callback_response_error)
                            raise db_op_callback_response_error

                    # If the database could not be updated
                    else:
                        self._operationErrorDB(callback_id, db_op_callback_error)
                        raise db_op_callback_error
            else:
                self._operationErrorDB(operation["id"], db_op_response_error)
                raise db_op_response_error
        # If the database could not be updated
        else:
            self._operationErrorDB(operation["id"], db_operation_error)
            raise db_operation_error

        return response

    def invokeParticipant(self, name: str, callback: t.Callable = None):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {
                "id": str(uuid.uuid4()),
                "type": "invokeParticipant",
                "method": self._invokeParticipant,
                "name": name,
                "callback": callback,
            }
        )

        return self

    def _withCompensation(self, operation):
        response = None
        operations = None

        if type(operation["name"]) == list:
            operations = operation["name"]
        else:
            operations = [operation["name"]]

        name = "_".join(operations)

        for compensation in operations:
            # Add current operation to lmdb
            (db_operation_flag, db_operation_error,) = self._createOperationDB(operation["id"], operation["type"], name)
            # if the DB was updated
            if db_operation_flag:
                try:
                    response = _withCompensation(compensation)
                except MinosSagaException as error:
                    raise error

                # Add response of current operation to lmdb
                (db_op_response_flag, db_op_response_error,) = self._operationResponseDB(operation["id"], response)

                # if the DB was updated with the response of previous operation
                if not db_op_response_flag:
                    self._operationErrorDB(operation["id"], db_op_response_error)
                    raise db_op_response_error
            # If the database could not be updated
            else:
                self._operationErrorDB(operation["id"], db_operation_error)
                raise db_operation_error

        if operation["callback"] is not None:

            func = operation["callback"]
            callback_id = str(uuid.uuid4())

            (db_op_callback_flag, db_op_callback_error,) = self._createOperationDB(
                callback_id, "withCompensation_callback", name
            )
            # if the DB was updated
            if db_op_callback_flag:
                try:
                    response = self.callback_function_call(func, response)
                except MinosSagaException as error:
                    self._operationErrorDB(callback_id, error)
                    raise error

                # Add response of current operation to lmdb
                (db_op_callback_response_flag, db_op_callback_response_error,) = self._operationResponseDB(
                    callback_id, response
                )

                # If the database could not be updated
                if not db_op_callback_response_flag:
                    self._operationErrorDB(callback_id, db_op_callback_response_error)
                    raise db_op_callback_response_error

            # If the database could not be updated
            else:
                self._operationErrorDB(callback_id, db_op_callback_error)
                raise db_op_callback_error

        return response

    def withCompensation(self, name: t.Union[str, list], callback: t.Callable = None):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {
                "id": str(uuid.uuid4()),
                "type": "withCompensation",
                "method": self._withCompensation,
                "name": name,
                "callback": callback,
            }
        )

        return self

    def _onReply(self, operation):
        response = None

        # Add current operation to lmdb
        (db_response_flag, db_response_error, db_response,) = self._getLastResponseDB()

        if db_response_flag:
            func = operation["callback"]
            callback_id = str(uuid.uuid4())

            (db_op_callback_flag, db_op_callback_error,) = self._createOperationDB(callback_id, operation["type"])

            if db_op_callback_flag:
                try:
                    response = self.callback_function_call(func, db_response)
                except MinosSagaException as error:
                    self._operationErrorDB(callback_id, error)
                    raise error

                # Add response of current operation to lmdb
                (db_op_callback_response_flag, db_op_callback_response_error,) = self._operationResponseDB(
                    callback_id, response
                )

                # If the database could not be updated
                if not db_op_callback_response_flag:
                    self._operationErrorDB(callback_id, db_op_callback_response_error)
                    raise db_op_callback_response_error
            else:
                self._operationErrorDB(callback_id, db_op_callback_error)
                raise db_op_callback_error
        else:
            self._operationErrorDB(operation["id"], db_response_error)
            raise db_response_error

        return response

    def onReply(self, _callback: t.Callable):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {"id": str(uuid.uuid4()), "type": "onReply", "method": self._onReply, "callback": _callback,}
        )

        return self

    def _execute(self):  # pragma: no cover
        pass

    def execute(self):
        self.saga_process["steps"][len(self.saga_process["steps"]) - 1].append(
            {"type": "execute", "method": self._execute}
        )
        self._validate_steps()
        self._execute_steps()

        return self

    def _validate_steps(self):
        for step in self.saga_process["steps"]:
            if not step:
                raise Exception("The step() cannot be empty.")

            for idx, operation in enumerate(step):
                if idx == 0 and operation["type"] != "invokeParticipant":
                    raise Exception(
                        "The first method of the step must be .invokeParticipant(name, callback (optional))."
                    )

    def _execute_steps(self):
        self._step_manager.start()

        for step in self.saga_process["steps"]:

            for operation in step:
                if operation["type"] == "withCompensation":
                    self.saga_process["current_compensations"].insert(0, operation)

            for operation in step:

                if operation["type"] == "invokeParticipant":
                    try:
                        self._response = self._invokeParticipant(operation)
                    except MinosSagaException as error:
                        self._rollback()
                        return self

                if operation["type"] == "onReply":
                    try:
                        self._response = self._onReply(operation)
                    except:
                        self._rollback()
                        return self

        self._step_manager.close()

    def _rollback(self):
        for operation in self.saga_process["current_compensations"]:
            self._withCompensation(operation)

        return self
