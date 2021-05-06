"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

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
from .context import (
    SagaContext,
)
from .executors import (
    InvokeParticipantExecutor,
    OnReplyExecutor,
    WithCompensationExecutor,
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
        executor = InvokeParticipantExecutor(storage, self._loop)
        try:
            context = executor.exec(self.definition.raw_invoke_participant, context)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedInvokeParticipant
            raise exc
        except MinosSagaException:
            self.status = SagaStepStatus.ErroredInvokeParticipant
            self.rollback(context, storage)
            raise MinosSagaFailedExecutionStepException()
        return context

    def _execute_on_reply(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        self.status = SagaStepStatus.RunningOnReply
        executor = OnReplyExecutor(storage, self._loop)
        # noinspection PyBroadException
        try:
            context = executor.exec(self.definition.raw_on_reply, context)
        except Exception:
            self.status = SagaStepStatus.ErroredOnReply
            self.rollback(context, storage)
            raise MinosSagaFailedExecutionStepException()
        return context

    def rollback(self, context: SagaContext, storage: MinosSagaStorage) -> SagaContext:
        """TODO

        :param context: TODO
        :param storage: TODO
        :return: TODO
        """
        if self.already_rollback:
            return context

        executor = WithCompensationExecutor(storage, self._loop)
        context = executor.exec(self.definition.raw_with_compensation, context)

        self.already_rollback = True
        return context

    @property
    def _loop(self):
        return self.execution.definition.loop
