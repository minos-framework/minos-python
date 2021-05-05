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


class SagaExecutedStep(object):
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
        pass

    def rollback(self) -> NoReturn:
        """TODO

        :return: TODO
        """
        pass
