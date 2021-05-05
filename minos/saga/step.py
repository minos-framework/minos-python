"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import inspect
from asyncio import (
    AbstractEventLoop,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    NoReturn,
    Optional,
    Union,
)
from uuid import (
    uuid4,
)

from .exceptions import (
    MinosMultipleInvokeParticipantException,
    MinosMultipleOnReplyException,
    MinosMultipleWithCompensationException,
    MinosSagaException,
)

if TYPE_CHECKING:
    from .step_manager import MinosSagaStepManager
    from .saga import Saga


def _invoke_participant(name):
    if name == "Shipping":
        raise MinosSagaException("invokeParticipantTest exception")

    return "_invokeParticipant Response"


# noinspection PyUnusedLocal
def _with_compensation(name):
    return "_withCompensation Response"


class SagaStep(object):
    """TODO"""

    def __init__(self, saga: Optional[Saga] = None):
        self.saga = saga
        self._invoke_participant = None
        self._with_compensation = None
        self._on_reply = None
        self._execute = None

    @property
    def raw(self) -> [dict[str, Any]]:
        """TODO

        :return: TODO
        """
        raw = list()
        if self._invoke_participant is not None:
            raw.append(self._invoke_participant)
        if self._with_compensation is not None:
            raw.append(self._with_compensation)
        if self._on_reply is not None:
            raw.append(self._on_reply)
        if self._execute is not None:
            raw.append(self._execute)

        return raw

    def invoke_participant(self, name: str, callback: Callable = None) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self._invoke_participant is not None:
            raise MinosMultipleInvokeParticipantException()

        self._invoke_participant = {
            "id": str(uuid4()),
            "type": "invokeParticipant",
            "method": self.execute_invoke_participant,
            "name": name,
            "callback": callback,
        }

        return self

    def execute_invoke_participant(self, operation):
        """TODO

        :param operation: TODO
        :return: TODO
        """
        # Add current operation to lmdb
        (db_operation_flag, db_operation_error,) = self._create_operation_db(
            operation["id"], operation["type"], operation["name"]
        )
        # if the DB was updated
        if db_operation_flag:
            try:
                response = _invoke_participant(operation["name"])
            except MinosSagaException as error:
                self._operation_error_db(operation["id"], error)
                raise error

            # Add response of current operation to lmdb
            (db_op_response_flag, db_op_response_error,) = self._operation_response_db(operation["id"], response)

            # if the DB was updated with the response of previous operation
            if db_op_response_flag:
                if operation["callback"] is not None:
                    func = operation["callback"]
                    callback_id = str(uuid4())

                    (db_op_callback_flag, db_op_callback_error,) = self._create_operation_db(
                        callback_id, "invokeParticipant_callback", operation["name"]
                    )
                    # if the DB was updated
                    if db_op_callback_flag:
                        try:
                            response = self._callback_function_call(func, self._response)
                        except MinosSagaException as error:
                            self._operation_error_db(callback_id, error)
                            raise error

                        # Add response of current operation to lmdb
                        (db_op_callback_response_flag, db_op_callback_response_error,) = self._operation_response_db(
                            callback_id, response
                        )

                        # If the database could not be updated
                        if not db_op_callback_response_flag:
                            self._operation_error_db(callback_id, db_op_callback_response_error)
                            raise db_op_callback_response_error

                    # If the database could not be updated
                    else:
                        self._operation_error_db(callback_id, db_op_callback_error)
                        raise db_op_callback_error
            else:
                self._operation_error_db(operation["id"], db_op_response_error)
                raise db_op_response_error
        # If the database could not be updated
        else:
            self._operation_error_db(operation["id"], db_operation_error)
            raise db_operation_error

        return response

    def with_compensation(self, name: Union[str, list], callback: Callable = None) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self._with_compensation is not None:
            raise MinosMultipleWithCompensationException()

        self._with_compensation = {
            "id": str(uuid4()),
            "type": "withCompensation",
            "method": self.execute_with_compensation,
            "name": name,
            "callback": callback,
        }

        return self

    def execute_with_compensation(self, operation):
        """TODO

        :param operation: TODO
        :return: TODO
        """
        response = None

        if type(operation["name"]) == list:
            operations = operation["name"]
        else:
            operations = [operation["name"]]

        name = "_".join(operations)

        for compensation in operations:
            # Add current operation to lmdb
            (db_operation_flag, db_operation_error,) = self._create_operation_db(
                operation["id"], operation["type"], name
            )
            # if the DB was updated
            if db_operation_flag:
                try:
                    response = _with_compensation(compensation)
                except MinosSagaException as error:
                    raise error

                # Add response of current operation to lmdb
                (db_op_response_flag, db_op_response_error,) = self._operation_response_db(operation["id"], response)

                # if the DB was updated with the response of previous operation
                if not db_op_response_flag:
                    self._operation_error_db(operation["id"], db_op_response_error)
                    raise db_op_response_error
            # If the database could not be updated
            else:
                self._operation_error_db(operation["id"], db_operation_error)
                raise db_operation_error

        if operation["callback"] is not None:
            func = operation["callback"]
            callback_id = str(uuid4())

            (db_op_callback_flag, db_op_callback_error,) = self._create_operation_db(
                callback_id, "withCompensation_callback", name
            )
            response = self._db_callback(db_op_callback_flag, callback_id, func, response, db_op_callback_error)

        return response

    def on_reply(self, _callback: Callable) -> SagaStep:
        """TODO

        :param _callback: TODO
        :return: TODO
        """
        if self._on_reply is not None:
            raise MinosMultipleOnReplyException()

        self._on_reply = {"id": str(uuid4()), "type": "onReply", "method": self.execute_on_reply, "callback": _callback}

        return self

    def execute_on_reply(self, operation):
        """TODO

        :param operation: TODO
        :return: TODO
        """

        # Add current operation to lmdb
        (db_response_flag, db_response_error, db_response,) = self._get_last_response_db()

        if db_response_flag:
            func = operation["callback"]
            callback_id = str(uuid4())

            (db_op_callback_flag, db_op_callback_error,) = self._create_operation_db(callback_id, operation["type"])

            response = self._db_callback(db_op_callback_flag, callback_id, func, db_response, db_op_callback_error)
        else:
            self._operation_error_db(operation["id"], db_response_error)
            raise db_response_error

        return response

    def _db_callback(self, db_op_callback_flag, callback_id, func, db_response, db_op_callback_error):
        if db_op_callback_flag:
            try:
                response = self._callback_function_call(func, db_response)
            except MinosSagaException as error:
                self._operation_error_db(callback_id, error)
                raise error

            # Add response of current operation to lmdb
            (db_op_callback_response_flag, db_op_callback_response_error,) = self._operation_response_db(
                callback_id, response
            )

            # If the database could not be updated
            if not db_op_callback_response_flag:
                self._operation_error_db(callback_id, db_op_callback_response_error)
                raise db_op_callback_response_error
        else:
            self._operation_error_db(callback_id, db_op_callback_error)
            raise db_op_callback_error
        return response

    @property
    def _response(self) -> str:
        return self.saga.response

    def _callback_function_call(self, func: Callable, response: str) -> Any:
        """TODO

        :param func: TODO
        :param response: TODO
        :return: TODO
        """
        task = func(response)
        if inspect.isawaitable(task):
            result = self._loop.run_until_complete(task)
            return result
        else:
            return task

    @property
    def _loop(self) -> AbstractEventLoop:
        return self.saga.loop

    def _create_operation_db(self, *args, **kwargs):
        return self._step_manager.create_operation_db(*args, **kwargs)

    def _operation_response_db(self, *args, **kwargs):
        return self._step_manager.operation_response_db(*args, **kwargs)

    def _operation_error_db(self, *args, **kwargs):
        return self._step_manager.operation_error_db(*args, **kwargs)

    def _get_last_response_db(self):
        return self._step_manager.get_last_response_db()

    @property
    def _step_manager(self) -> MinosSagaStepManager:
        return self.saga.step_manager

    def step(self) -> SagaStep:
        """TODO

        :return: TODO
        """
        self.validate()
        return self.saga.step()

    def execute(self) -> Saga:
        """TODO

        :return: TODO
        """
        self._execute = {"type": "execute", "method": self._execute}
        return self.saga.execute()

    def validate(self) -> NoReturn:
        """TODO

        :return TODO:
        """
        if not self.raw:
            raise MinosSagaException("The step() cannot be empty.")

        for idx, operation in enumerate(self.raw):
            if idx == 0 and operation["type"] != "invokeParticipant":
                raise MinosSagaException(
                    "The first method of the step must be .invokeParticipant(name, callback (optional))."
                )
