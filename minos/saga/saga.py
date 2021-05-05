"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

import asyncio
import typing as t
import uuid

from .abstract import (
    MinosBaseSagaBuilder,
)
from .exceptions import (
    MinosSagaException,
)
from .step import (
    SagaStep,
)
from .step_manager import (
    MinosSagaStepManager,
)


class Saga(MinosBaseSagaBuilder):
    """TODO"""

    def __init__(
        self,
        name,
        db_path: str = "./db.lmdb",
        step_manager: t.Type[MinosSagaStepManager] = MinosSagaStepManager,
        loop: asyncio.AbstractEventLoop = None,
    ):
        self.saga_name = name
        self.uuid = str(uuid.uuid4())
        self.saga_process = {
            "name": self.saga_name,
            "id": self.uuid,
            "steps": [],
            "current_compensations": [],
        }
        self.step_manager = step_manager(self.saga_name, self.uuid, db_path)
        self.loop = loop or asyncio.get_event_loop()
        self.response = ""
        self._steps = list()

    def get_db_state(self) -> dict[str, t.Any]:
        """TODO

        :return: TODO
        """
        return self.step_manager.get_state()

    def step(self, step: t.Optional[SagaStep] = None) -> SagaStep:
        """TODO

        :return: TODO
        """
        if step is None:
            step = SagaStep(self)
        else:
            if step.saga is not None:
                raise ValueError()
            step.saga = self

        self._steps.append(step)
        return step

    def execute(self) -> Saga:
        """TODO

        :return: TODO
        """
        self.saga_process["steps"] = [step.raw for step in self._steps]
        self._execute_steps()

        return self

    def _execute_steps(self):
        self.step_manager.start()

        for step in self._steps:

            for operation in step.raw:
                if operation["type"] == "withCompensation":
                    self.saga_process["current_compensations"].insert(0, operation)

            for operation in step.raw:

                if operation["type"] == "invokeParticipant":
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
        for operation in self.saga_process["current_compensations"]:
            self._steps[-1].execute_with_compensation(operation)

        return self
