"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import uuid
from typing import (
    Any,
)

from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class OnReplyExecutor(LocalExecutor):
    """TODO"""

    def exec(self, operation: dict[str, Any], context: SagaContext):
        """TODO

        :param operation: TODO
        :param context: TODO
        :return: TODO
        """
        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": operation["type"],
            "name": "",
            "callback": operation["callback"],
        }
        context = super().exec(callback_operation, context)

        return context
