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
from .status import (
    SagaStatus,
)

if TYPE_CHECKING:
    from .saga import (
        SagaExecution,
    )


class SagaStepExecution(object):
    """TODO"""

    def __init__(self, execution: SagaExecution, definition: SagaStep, status: SagaStatus = SagaStatus.Created):
        self.execution = execution
        self.definition = definition
        self.status = status

    def execute(self):
        """TODO

        :return: TODO
        """
        pass
