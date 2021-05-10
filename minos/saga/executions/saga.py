"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Any,
    Iterable,
    NoReturn,
    Optional,
)
from uuid import (
    UUID,
)

from ..definitions import (
    Saga,
    SagaStep,
)
from ..exceptions import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
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
        already_rollback: bool = False,
        *args,
        **kwargs
    ):

        self.uuid = uuid
        self.definition = definition
        self.executed_steps = steps
        self.context = context
        self.status = status
        self.already_rollback = already_rollback

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
        from uuid import (
            uuid4,
        )

        return cls(definition, uuid4(), list(), SagaContext(), SagaStatus.Created, *args, **kwargs)

    def execute(self, response: Optional[Any] = None):
        """TODO

        :param response: TODO
        :return: TODO
        """
        self.status = SagaStatus.Running
        for step in self.pending_steps:
            execution_step = SagaExecutionStep(self, step)
            try:
                self.context = execution_step.execute(self.context, response=response)
                self._add_executed(execution_step)
            except MinosSagaFailedExecutionStepException as exc:
                self.rollback()
                self.status = SagaStatus.Errored
                raise exc
            except MinosSagaPausedExecutionStepException as exc:
                self.status = SagaStatus.Paused
                raise exc

            response = None  # Response is consumed

        self.status = SagaStatus.Finished
        return self.context

    def rollback(self) -> NoReturn:
        """TODO

        :return: TODO
        """

        if self.already_rollback:
            return

        for execution_step in reversed(self.executed_steps):
            self.context = execution_step.rollback(self.context)

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

    @property
    def raw(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return {
            "definition": self.definition.raw,
            "uuid": str(self.uuid),
            "status": self.status.raw,
            "executed_steps": [step.raw for step in self.executed_steps],
            "context": self.context,
            "already_rollback": self.already_rollback,
        }
