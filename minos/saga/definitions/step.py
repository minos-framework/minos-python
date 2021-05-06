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
    from ..storage import (
        MinosSagaStorage,
    )
    from .saga import (
        Saga,
    )


def _invoke_participant(name) -> SagaContext:
    if name == "Shipping":
        raise MinosSagaException("invokeParticipantTest exception")

    # noinspection PyTypeChecker
    return "_invokeParticipant Response"


# noinspection PyUnusedLocal
def _with_compensation(name) -> SagaContext:
    # noinspection PyTypeChecker
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

    def execute_invoke_participant(self, context: SagaContext, storage: MinosSagaStorage):
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        operation = self.raw_invoke_participant
        if operation is None:
            return context

        storage.create_operation(operation)
        try:
            context = _invoke_participant(operation["name"])
        except MinosSagaException as error:
            storage.operation_error_db(operation["id"], error)
            raise error
        storage.store_operation_response(operation["id"], context)

        if operation["callback"] is None:
            return context

        callback_operation = {"id": str(uuid.uuid4()), "type": "invokeParticipant_callback", "name": operation["name"]}
        context = self._exec(callback_operation, operation, context, storage)

        return context

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

    def execute_with_compensation(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        operation = self.raw_with_compensation
        if operation is None:
            return context

        storage.create_operation(operation)
        try:
            context = _with_compensation(operation["name"])
        except MinosSagaException as error:
            raise error
        storage.store_operation_response(operation["id"], context)

        if operation["callback"] is None:
            return context

        callback_operation = {"id": str(uuid.uuid4()), "type": "withCompensation_callback", "name": operation["name"]}
        context = self._exec(callback_operation, operation, context, storage)

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

    def execute_on_reply(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        operation = self.raw_on_reply
        # Add current operation to lmdb

        callback_operation = {"id": str(uuid.uuid4()), "type": operation["type"], "name": ""}
        context = self._exec(callback_operation, operation, context, storage)

        return context

    def _exec(
        self,
        callback_operation: dict[str, Any],
        operation: dict[str, Any],
        context: SagaContext,
        storage: MinosSagaStorage,
    ):
        storage.create_operation(callback_operation)
        try:
            context = self._exec_function(operation["callback"], context)
        except MinosSagaException as error:
            storage.operation_error_db(callback_operation["id"], error)
            raise error
        storage.store_operation_response(callback_operation["id"], context)

        return context

    def _exec_function(self, func: Callable, context: SagaContext) -> SagaContext:
        """TODO

        :param func: TODO
        :param context: TODO
        :return: TODO
        """
        result = func(context)
        if inspect.isawaitable(result):
            result = self._loop.run_until_complete(result)
            return result
        return result

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
