"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
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
    SagaExecutedStep,
)


class SagaExecution(object):
    """TODO"""

    def __init__(
        self, definition: Saga, uuid: UUID, steps: [SagaExecutedStep], context: SagaContext, status: SagaStatus
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
        self.step_manager.start()

        for step in self.pending_steps:
            try:
                executed_step = SagaExecutedStep(self, step)
                self.add_executed_step(executed_step)
            except ValueError:  # FIXME: Exception that pauses execution.
                break
            except TypeError:  # FIXME: Exception that rollbacks execution.
                self.rollback()
                break

        self.step_manager.close()

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
        return []

    def add_executed_step(self, executed_step: SagaExecutedStep):
        """TODO

        :param executed_step: TODO
        :return: TODO
        """
        self.steps.append(executed_step)

    @property
    def step_manager(self):
        """TODO

        :return: TODO
        """
        return self.definition.step_manager
