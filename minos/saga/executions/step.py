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

    def execute(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        for operation in self.definition.raw:
            if operation["type"] == "withCompensation":
                self.execution.definition.saga_process["current_compensations"].insert(0, operation)

        for operation in self.definition.raw:

            if operation["type"] == "invokeParticipant":
                from minos.saga import (
                    MinosSagaException,
                )

                try:
                    self.execution.response = self.definition.execute_invoke_participant(operation)
                except MinosSagaException:
                    self.execution._rollback()
                    return self

            if operation["type"] == "onReply":
                # noinspection PyBroadException
                try:
                    self.execution.response = self.definition.execute_on_reply(operation)
                except Exception:
                    self.execution._rollback()
                    return self

    def rollback(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        pass
