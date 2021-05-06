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
from pathlib import (
    Path,
)

from ..exceptions import (
    MinosSagaException,
)
from ..step_manager import (
    MinosSagaStepManager,
)
from .abc import (
    MinosBaseSagaBuilder,
)
from .step import (
    SagaStep,
)

if t.TYPE_CHECKING:
    from ..executions import (
        SagaExecution,
    )


class Saga(MinosBaseSagaBuilder):
    """TODO"""

    def __init__(
        self,
        name,
        db_path: t.Union[Path, str] = "./db.lmdb",
        step_manager: t.Type[MinosSagaStepManager] = MinosSagaStepManager,
        loop: asyncio.AbstractEventLoop = None,
    ):
        if not isinstance(db_path, str):
            db_path = str(db_path)

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

    def build_execution(self) -> SagaExecution:
        """TODO

        :return: TODO
        """
        from ..executions import (
            SagaExecution,
        )

        return SagaExecution.from_saga(self)
