"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Any,
    NoReturn,
)
from uuid import (
    UUID,
    uuid4,
)

from ..definitions import (
    Saga,
    SagaStep,
)
from .context import (
    SagaContext,
)
from .status import (
    SagaStatus,
)
from .step import (
    SagaExecutionStep,
)


class SagaExecution(object):
    """TODO"""

    def __init__(
        self, definition: Saga, uuid: UUID, steps: [SagaExecutionStep], context: SagaContext, status: SagaStatus
    ):
        self.uuid = uuid
        self.definition = definition
        self.steps = steps
        self.context = context
        self.status = status
        self.already_rollback = False

    @classmethod
    def from_saga(cls, definition: Saga):
        """TODO

        :param definition: TODO
        :return: TODO
        """
        return cls(definition, uuid=uuid4(), steps=list(), context=SagaContext(), status=SagaStatus.Created)

    def execute(self):
        """TODO

        :return: TODO
        """
        self.definition.saga_process["steps"] = [step.raw for step in self.definition._steps]

        self.step_manager.start()

        for step in self.pending_steps:
            # try:
            #     execution_step = SagaExecutedStep(self, step)
            #     self.add_executed_step(execution_step)
            # except ValueError:  # FIXME: Exception that pauses execution.
            #     break
            # except TypeError:  # FIXME: Exception that rollbacks execution.
            #     self.rollback()
            #     break

            # execution_step = SagaExecutionStep(self, step)
            # execution_step.execute()
            # self.add_executed_step(execution_step)
            for operation in step.raw:
                if operation["type"] == "withCompensation":
                    self.definition.saga_process["current_compensations"].insert(0, operation)

            for operation in step.raw:

                if operation["type"] == "invokeParticipant":
                    from minos.saga import (
                        MinosSagaException,
                    )

                    try:
                        self.response = step.execute_invoke_participant(operation)
                    except MinosSagaException:
                        self._rollback()
                        return self

                if operation["type"] == "onReply":
                    # noinspection PyBroadException
                    try:
                        self.response = step.execute_on_reply(operation)
                    except Exception:
                        self._rollback()
                        return self

        self.step_manager.close()

    def _rollback(self):
        for operation in self.definition.saga_process["current_compensations"]:
            self.definition._steps[-1].execute_with_compensation(operation)

        return self

    def rollback(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        if self.already_rollback:
            return

        for executed_step in reversed(self.steps):
            executed_step.rollback()

        self.already_rollback = True

    @property
    def pending_steps(self) -> [SagaStep]:
        """TODO

        :return: TODO
        """
        return self.definition._steps

    def add_executed_step(self, executed_step: SagaExecutionStep):
        """TODO

        :param executed_step: TODO
        :return: TODO
        """
        self.steps.append(executed_step)

    def get_db_state(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return self.step_manager.get_state()

    @property
    def step_manager(self):
        """TODO

        :return: TODO
        """
        return self.definition.step_manager
