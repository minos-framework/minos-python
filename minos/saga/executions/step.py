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
    NoReturn,
)

from ..definitions import (
    SagaStep,
)
from ..exceptions import (
    MinosSagaFailedExecutionStepException,
)
from ..step_manager import (
    MinosSagaStepManager,
)
from .status import (
    SagaStatus,
)

if TYPE_CHECKING:
    from .saga import (
        SagaExecution,
    )


class SagaExecutionStep(object):
    """TODO"""

    def __init__(self, execution: SagaExecution, definition: SagaStep, status: SagaStatus = SagaStatus.Created):
        self.execution = execution
        self.definition = definition
        self.status = status
        self.already_rollback = False

    def execute(self, step_manager: MinosSagaStepManager) -> NoReturn:
        """TODO

        :param step_manager: TODO
        :return: TODO
        """

        self.execution.definition.saga_process["steps"].append(self.definition.raw)

        for operation in self.definition.raw:
            if operation["type"] == "withCompensation":
                self.execution.definition.saga_process["current_compensations"].insert(0, operation)

        for operation in self.definition.raw:

            if operation["type"] == "invokeParticipant":
                from minos.saga import (
                    MinosSagaException,
                )

                try:
                    self.execution.definition.response = self.definition.execute_invoke_participant(
                        operation, step_manager
                    )
                except MinosSagaException:
                    self.rollback(step_manager)
                    raise MinosSagaFailedExecutionStepException()

            if operation["type"] == "onReply":
                # noinspection PyBroadException
                try:
                    self.execution.definition.response = self.definition.execute_on_reply(operation, step_manager)
                except Exception:
                    self.rollback(step_manager)
                    raise MinosSagaFailedExecutionStepException()

    def rollback(self, step_manager: MinosSagaStepManager) -> NoReturn:
        """TODO

        :param step_manager: TODO
        :return: TODO
        """

        step = self.definition
        operation = step._with_compensation
        if operation is not None:
            step.execute_with_compensation(operation, step_manager)
