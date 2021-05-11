"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Any,
)

from minos.common import (
    CommandReply,
)

from ...definitions import (
    SagaStepOperation,
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
    def exec(self, operation: SagaStepOperation, context: SagaContext, reply: CommandReply, *args, **kwargs):
        """TODO

        :param operation: TODO
        :param context: TODO
        :param reply: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if operation is None:
            return context

        if reply is None:
            raise MinosSagaPausedExecutionStepException()

        value = reply.items
        if len(value) == 1:
            value = value[0]

        response = super().exec_one(operation, value)
        context.update(operation.name, response)
        return context
