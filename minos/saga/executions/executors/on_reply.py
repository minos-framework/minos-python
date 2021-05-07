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

from ...exceptions import (
    MinosSagaPausedExecutionStepException,
)
from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class OnReplyExecutor(LocalExecutor):
    """TODO"""

    # noinspection PyUnusedLocal
    def exec(self, operation: dict[str, Any], context: SagaContext, response: Aggregate = None, *args, **kwargs):
        """TODO

        :param operation: TODO
        :param context: TODO
        :param response: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if operation is None:
            return context

        if response is None:
            raise MinosSagaPausedExecutionStepException()

        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": operation["type"],
            "name": operation["name"],
            "callback": operation["callback"],
        }
        response = super().exec_one(callback_operation, response)
        context.update(operation["name"], response)
        return context
