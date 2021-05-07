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
from ..exceptions import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
)
from ..storage import (
    MinosSagaStorage,
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

    # noinspection PyUnusedLocal
    def __init__(
        self,
        definition: Saga,
        uuid: UUID,
        steps: [SagaExecutionStep],
        context: SagaContext,
        status: SagaStatus,
        *args,
        **kwargs
    ):

        self.uuid = uuid
        self.definition = definition
        self.executed_steps = steps
        # self.context = context  # FIXME
        self.context = ""
        self.status = status
        self.already_rollback = False

        self.saga_process = {
            "name": self.definition.name,
            "id": self.uuid,
            "steps": [],
            "current_compensations": [],
        }

    @classmethod
    def from_saga(cls, definition: Saga, *args, **kwargs):
        """TODO

        :param definition: TODO
        :return: TODO
        """
        return cls(definition, uuid4(), list(), SagaContext(), SagaStatus.Created, *args, **kwargs)

    def execute(self, storage: MinosSagaStorage):
        """TODO

        :param storage: TODO
        :return: TODO
        """
        self.status = SagaStatus.Running
        for step in self.pending_steps:
            execution_step = SagaExecutionStep(self, step)
            try:
                self.context = execution_step.execute(self.context, storage)
            except MinosSagaFailedExecutionStepException:  # FIXME: Exception that rollbacks execution.
                self.rollback(storage)
                self.status = SagaStatus.Errored
                return self
            except MinosSagaPausedExecutionStepException:  # FIXME: Exception that pauses execution.
                self.status = SagaStatus.Paused
                return self
            finally:
                self._add_executed(execution_step)

        self.status = SagaStatus.Finished

    def rollback(self, storage: MinosSagaStorage) -> NoReturn:
        """TODO

        :param storage: TODO
        :return: TODO
        """

        if self.already_rollback:
            return

        for execution_step in reversed(self.executed_steps):
            self.context = execution_step.rollback(self.context, storage)

        self.already_rollback = True

    @property
    def pending_steps(self) -> [SagaStep]:
        """TODO

        :return: TODO
        """
        offset = len(self.executed_steps)
        return self.definition.steps[offset:]

    def _add_executed(self, executed_step: SagaExecutionStep):
        """TODO

        :param executed_step: TODO
        :return: TODO
        """
        self.executed_steps.append(executed_step)
