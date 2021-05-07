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
    Any,
    NoReturn,
    Optional,
)

from ..definitions import (
    SagaStep,
)
from ..exceptions import (
    MinosSagaException,
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
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

    def execute(self, context: SagaContext, response: Optional[Any] = None) -> SagaContext:
        """TODO

        :param context: TODO
        :param response: TODO
        :return: TODO
        """

        self._register()

        self._execute_invoke_participant(context)
        context = self._execute_on_reply(context, response)

        self.status = SagaStepStatus.Finished
        return context

    def _register(self) -> NoReturn:
        self.execution.saga_process["steps"].append(self.definition.raw)
        operation = self.definition.raw_with_compensation
        if operation is not None:
            self.execution.saga_process["current_compensations"].insert(0, operation)

    def _execute_invoke_participant(self, context: SagaContext) -> NoReturn:
        if self.status != SagaStepStatus.Created:
            return

        self.status = SagaStepStatus.RunningInvokeParticipant
        executor = InvokeParticipantExecutor(self._loop)
        try:
            executor.exec(self.definition.raw_invoke_participant, context)
        except MinosSagaException:
            self.status = SagaStepStatus.ErroredInvokeParticipant
            self.rollback(context)
            raise MinosSagaFailedExecutionStepException()
        self.status = SagaStepStatus.FinishedInvokeParticipant

    def _execute_on_reply(self, context: SagaContext, response: Optional[Any] = None) -> SagaContext:
        self.status = SagaStepStatus.RunningOnReply
        executor = OnReplyExecutor(self._loop)
        # noinspection PyBroadException
        try:
            context = executor.exec(self.definition.raw_on_reply, context, response=response)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedOnReply
            raise exc
        except Exception as e:
            self.status = SagaStepStatus.ErroredOnReply
            self.rollback(context)
            raise MinosSagaFailedExecutionStepException()
        return context

    def rollback(self, context: SagaContext) -> SagaContext:
        """TODO

        :param context: TODO
        :return: TODO
        """
        if self.already_rollback:
            return context

        executor = WithCompensationExecutor(self._loop)
        executor.exec(self.definition.raw_with_compensation, context)

        self.already_rollback = True
        return context

    @property
    def _loop(self):
        return self.execution.definition.loop
