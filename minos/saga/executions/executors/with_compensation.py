"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import (
    Aggregate,
)

from ...definitions import (
    SagaStepOperation,
)
from ..context import (
    SagaContext,
)
from .publish import (
    PublishExecutor,
)


class WithCompensationExecutor(PublishExecutor):
    """TODO"""

    def _run_callback(self, operation: SagaStepOperation, context: SagaContext) -> Aggregate:
        return super().exec_one(operation, context)
