"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import uuid
from typing import (
    TYPE_CHECKING,
)

from ..definitions import (
    SagaStep,
)
from ..exceptions import (
    MinosSagaException,
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
)
from ..storage import (
    MinosSagaStorage,
)
from . import (
    LocalExecutor,
)
from .context import (
    SagaContext,
)
from .status import (
    SagaStepStatus,
)

if TYPE_CHECKING:
    from .saga import (
        SagaExecution,
    )


class SagaExecutionStep(object):
    """TODO"""

    def __init__(self, execution: SagaExecution, definition: SagaStep, status: SagaStepStatus = SagaStepStatus.Created):
        self.execution = execution
        self.definition = definition
        self.status = status
        self.already_rollback = False

    def execute(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """

        self.execution.saga_process["steps"].append(self.definition.raw)
        self._register_with_compensation()

        context = self._execute_invoke_participant(context, storage)

        context = self._execute_on_reply(context, storage)

        self.status = SagaStepStatus.Finished
        return context

    def _register_with_compensation(self):
        operation = self.definition.raw_with_compensation
        if operation is not None:
            self.execution.saga_process["current_compensations"].insert(0, operation)

    def _execute_invoke_participant(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        self.status = SagaStepStatus.RunningInvokeParticipant
        try:
            context = self.execute_invoke_participant(context, storage)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedInvokeParticipant
            raise exc
        except MinosSagaException:
            self.status = SagaStepStatus.ErroredInvokeParticipant
            self.rollback(context, storage)
            raise MinosSagaFailedExecutionStepException()
        return context

    def execute_invoke_participant(self, context: SagaContext, storage: MinosSagaStorage):
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        operation = self.definition.raw_invoke_participant
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

        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": "invokeParticipant_callback",
            "name": operation["name"],
            "callback": operation["callback"],
        }
        context = LocalExecutor(self._loop).exec(callback_operation, context, storage)

        return context

    def _execute_on_reply(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        self.status = SagaStepStatus.RunningOnReply
        # noinspection PyBroadException
        try:
            context = self.execute_on_reply(context, storage)
        except Exception:
            self.status = SagaStepStatus.ErroredOnReply
            self.rollback(context, storage)
            raise MinosSagaFailedExecutionStepException()
        return context

    def execute_on_reply(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        operation = self.definition.raw_on_reply
        # Add current operation to lmdb

        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": operation["type"],
            "name": "",
            "callback": operation["callback"],
        }
        context = LocalExecutor(self._loop).exec(callback_operation, context, storage)

        return context

    def rollback(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        if self.already_rollback:
            return context

        context = self.execute_with_compensation(context, storage)

        self.already_rollback = True
        return context

    def execute_with_compensation(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        operation = self.definition.raw_with_compensation
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

        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": "withCompensation_callback",
            "name": operation["name"],
            "callback": operation["callback"],
        }
        context = LocalExecutor(self._loop).exec(callback_operation, context, storage)

        return context

    @property
    def _loop(self):
        return self.execution.definition.loop


def _invoke_participant(name) -> SagaContext:
    if name == "Shipping":
        raise MinosSagaException("invokeParticipantTest exception")

    # noinspection PyTypeChecker
    return "_invokeParticipant Response"


# noinspection PyUnusedLocal
def _with_compensation(name) -> SagaContext:
    # noinspection PyTypeChecker
    return "_withCompensation Response"
