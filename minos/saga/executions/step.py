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
from ..step_manager import (
    MinosSagaStepManager,
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

    def execute(self, context: SagaContext, step_manager: MinosSagaStepManager) -> SagaContext:
        """TODO

        :param context: TODO
        :param step_manager: TODO
        :return: TODO
        """

        self.execution.definition.saga_process["steps"].append(self.definition.raw)
        self._register_with_compensation()

        context = self._execute_invoke_participant(context, step_manager)

        context = self._execute_on_reply(context, step_manager)

        self.status = SagaStepStatus.Finished
        return context

    def _register_with_compensation(self):
        operation = self.definition.raw_with_compensation
        if operation is not None:
            self.execution.definition.saga_process["current_compensations"].insert(0, operation)

    def _execute_invoke_participant(self, context: SagaContext, step_manager: MinosSagaStepManager) -> SagaContext:
        self.status = SagaStepStatus.RunningInvokeParticipant
        try:
            context = self.definition.execute_invoke_participant(context, step_manager)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedInvokeParticipant
            raise exc
        except MinosSagaException:
            self.status = SagaStepStatus.ErroredInvokeParticipant
            self.rollback(context, step_manager)
            raise MinosSagaFailedExecutionStepException()
        return context

    def _execute_on_reply(self, context: SagaContext, step_manager: MinosSagaStepManager) -> SagaContext:
        self.status = SagaStepStatus.RunningOnReply
        # noinspection PyBroadException
        try:
            context = self.definition.execute_on_reply(context, step_manager)
        except Exception:
            self.status = SagaStepStatus.ErroredOnReply
            self.rollback(context, step_manager)
            raise MinosSagaFailedExecutionStepException()
        return context

    def rollback(self, context: SagaContext, step_manager: MinosSagaStepManager) -> SagaContext:
        """TODO

        :param context: TODO
        :param step_manager: TODO
        :return: TODO
        """
        if self.already_rollback:
            return context

        context = self.definition.execute_with_compensation(context, step_manager)

        self.already_rollback = True
        return context
