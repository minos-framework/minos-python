"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import uuid
from typing import (
    Any,
)

from minos.common import (
    Aggregate,
)

from ..context import (
    SagaContext,
)
from .publish import (
    PublishExecutor,
)


class WithCompensationExecutor(PublishExecutor):
    """TODO"""

    def _run_callback(self, operation: dict[str, Any], context: SagaContext) -> Aggregate:
        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": "withCompensation_callback",
            "name": operation["name"],
            "callback": operation["callback"],
        }
        return super().exec_one(callback_operation, context)
