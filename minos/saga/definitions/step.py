"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import inspect
import uuid
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

from ..exceptions import (
    MinosMultipleInvokeParticipantException,
    MinosMultipleOnReplyException,
    MinosMultipleWithCompensationException,
    MinosSagaEmptyStepException,
    MinosSagaException,
    MinosSagaNotDefinedException,
)

if TYPE_CHECKING:
    from ..executions import (
        SagaContext,
    )
    from ..step_manager import (
        MinosSagaStepManager,
    )
    from .saga import (
        Saga,
    )


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
        self.raw_invoke_participant = None
        self.raw_with_compensation = None
        self.raw_on_reply = None
        self._execute = None

    @property
    def raw(self) -> [dict[str, Any]]:
        """TODO

        :return: TODO
        """
        raw = list()
        if self.raw_invoke_participant is not None:
            raw.append(self.raw_invoke_participant)
        if self.raw_with_compensation is not None:
            raw.append(self.raw_with_compensation)
        if self.raw_on_reply is not None:
            raw.append(self.raw_on_reply)
        if self._execute is not None:
            raw.append(self._execute)

        return raw

    def invoke_participant(self, name: str, callback: Callable = None) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.raw_invoke_participant is not None:
            raise MinosMultipleInvokeParticipantException()

        self.raw_invoke_participant = {
            "id": str(uuid.uuid4()),
            "type": "invokeParticipant",
            "name": name,
            "callback": callback,
        }

        return self

    def execute_invoke_participant(self, context: SagaContext, step_manager: MinosSagaStepManager):
        """TODO

        :param context: TODO
        :param step_manager: TODO
        :return: TODO
        """
        operation = self.raw_invoke_participant
        if operation is None:
            return context

        step_manager.create_operation(operation)
        try:
            response = _invoke_participant(operation["name"])
        except MinosSagaException as error:
            step_manager.operation_error_db(operation["id"], error)
            raise error
        step_manager.store_operation_response(operation["id"], response)

        if operation["callback"] is None:
            return response

        callback_operation = {"id": str(uuid.uuid4()), "type": "invokeParticipant_callback", "name": operation["name"]}
        step_manager.create_operation(callback_operation)
        try:
            response = self._callback_function_call(operation["callback"], context)
        except MinosSagaException as error:
            step_manager.operation_error_db(callback_operation["id"], error)
            raise error
        step_manager.store_operation_response(callback_operation["id"], response)

        return response

    def with_compensation(self, name: Union[str, list], callback: Callable = None) -> SagaStep:
        """TODO

        :param name: TODO
        :param callback: TODO
        :return: TODO
        """
        if self.raw_with_compensation is not None:
            raise MinosMultipleWithCompensationException()

        self.raw_with_compensation = {
            "id": str(uuid.uuid4()),
            "type": "withCompensation",
            "name": name,
            "callback": callback,
        }

        return self

    def execute_with_compensation(self, context: SagaContext, step_manager: MinosSagaStepManager) -> SagaContext:
        """TODO

        :param context: TODO
        :param step_manager: TODO
        :return: TODO
        """
        operation = self.raw_with_compensation
        if operation is None:
            return context

        response = None

        if type(operation["name"]) == list:
            operations = operation["name"]
        else:
            operations = [operation["name"]]

        name = "_".join(operations)

        for compensation in operations:
            # Add current operation to lmdb
            (db_operation_flag, db_operation_error,) = step_manager.create_operation_db(
                operation["id"], operation["type"], name
            )
            # if the DB was updated
            if db_operation_flag:
                try:
                    response = _with_compensation(compensation)
                except MinosSagaException as error:
                    raise error

                # Add response of current operation to lmdb
                (db_op_response_flag, db_op_response_error,) = step_manager.operation_response_db(
                    operation["id"], response
                )

                # if the DB was updated with the response of previous operation
                if not db_op_response_flag:
                    step_manager.operation_error_db(operation["id"], db_op_response_error)
                    raise db_op_response_error
            # If the database could not be updated
            else:
                step_manager.operation_error_db(operation["id"], db_operation_error)
                raise db_operation_error

        if operation["callback"] is not None:
            func = operation["callback"]
            callback_id = str(uuid.uuid4())

            (db_op_callback_flag, db_op_callback_error,) = step_manager.create_operation_db(
                callback_id, "withCompensation_callback", name
            )
            context = self._db_callback(
                db_op_callback_flag, callback_id, func, response, db_op_callback_error, context, step_manager
            )

        return context

    def on_reply(self, _callback: Callable) -> SagaStep:
        """TODO

        :param _callback: TODO
        :return: TODO
        """
        if self.raw_on_reply is not None:
            raise MinosMultipleOnReplyException()

        self.raw_on_reply = {
            "id": str(uuid.uuid4()),
            "type": "onReply",
            "callback": _callback,
        }

        return self

    def execute_on_reply(self, context: SagaContext, step_manager: MinosSagaStepManager) -> SagaContext:
        """TODO

        :param context: TODO
        :param step_manager: TODO
        :return: TODO
        """
        operation = self.raw_on_reply
        # Add current operation to lmdb
        (db_response_flag, db_response_error, db_response,) = step_manager.get_last_response_db()

        if db_response_flag:
            func = operation["callback"]
            callback_id = str(uuid.uuid4())

            (db_op_callback_flag, db_op_callback_error,) = step_manager.create_operation_db(
                callback_id, operation["type"]
            )

            context = self._db_callback(
                db_op_callback_flag, callback_id, func, db_response, db_op_callback_error, context, step_manager
            )
        else:
            step_manager.operation_error_db(operation["id"], db_response_error)
            raise db_response_error

        return context

    def _db_callback(
        self,
        db_op_callback_flag,
        callback_id,
        func,
        db_response,
        db_op_callback_error,
        context: SagaContext,
        step_manager: MinosSagaStepManager,
    ):
        if db_op_callback_flag:
            try:
                response = self._callback_function_call(func, db_response)
            except MinosSagaException as error:
                step_manager.operation_error_db(callback_id, error)
                raise error

            # Add response of current operation to lmdb
            (db_op_callback_response_flag, db_op_callback_response_error,) = step_manager.operation_response_db(
                callback_id, response
            )

            # If the database could not be updated
            if not db_op_callback_response_flag:
                step_manager.operation_error_db(callback_id, db_op_callback_response_error)
                raise db_op_callback_response_error
        else:
            step_manager.operation_error_db(callback_id, db_op_callback_error)
            raise db_op_callback_error
        return context

    def _callback_function_call(self, func: Callable, response: Any) -> Any:
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

    def step(self) -> SagaStep:
        """TODO

        :return: TODO
        """
        self.validate()
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga.step()

    def commit(self) -> Saga:
        """TODO

        :return: TODO
        """
        if self.saga is None:
            raise MinosSagaNotDefinedException()
        return self.saga

    def validate(self) -> NoReturn:
        """TODO

        :return TODO:
        """
        if not self.raw:
            raise MinosSagaEmptyStepException()

        for idx, operation in enumerate(self.raw):
            if idx == 0 and operation["type"] != "invokeParticipant":
                raise MinosSagaException(
                    "The first method of the step must be .invokeParticipant(name, callback (optional))."
                )
